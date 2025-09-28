"""Upgrade command for PolicyStack CLI."""

import asyncio
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from ruamel.yaml import YAML

from ..core.change_detector import ChangeDetector, TemplateSnapshot
from ..core.conflict_resolver import ConflictReport, ConflictResolver, ConflictMarkerGenerator
from ..core.merger import YAMLMerger, HelmTemplateMerger, MergeConflict, PreservingYAMLMerger
from ..core.installer import TemplateInstaller


class TemplateUpgrader:
    """Handles the complete upgrade process for templates."""
    
    def __init__(self, marketplace, console: Console):
        self.marketplace = marketplace
        self.console = console
        self.installer = TemplateInstaller(marketplace, console)
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.width = 4096
        self.yaml.indent(mapping=2, sequence=2, offset=0)
 
    async def download_version(self, template_name: str, version: str, repository: str = None) -> Path:
        """Download a specific template version to a temporary directory."""
        template = await self.marketplace.get_template(template_name, repository)
        if not template:
            raise ValueError(f"Template {template_name} not found")
        
        repo = self.marketplace.get_repository(template.repository)
        if not repo:
            raise ValueError(f"Repository {template.repository} not found")
        
        # Create temp directory for new version
        temp_dir = Path(tempfile.mkdtemp(prefix=f"policystack_upgrade_{template_name}_"))
        
        # Download template files using git handler
        if repo.is_git:
            from ..core.git_repository import GitRepositoryHandler
            git_handler = GitRepositoryHandler(self.marketplace.cache_dir)
            
            success, template_dir, message = git_handler.get_template_files(
                url=repo.url,
                template_path=template.path,
                version=version,
                branch=repo.branch,
                auth_token=repo.auth_token
            )
            
            if not success or not template_dir:
                shutil.rmtree(temp_dir)
                raise Exception(f"Failed to download template: {message}")
            
            # Move files to our temp directory
            shutil.copytree(template_dir, temp_dir, dirs_exist_ok=True)
            shutil.rmtree(template_dir)
        
        return temp_dir
    
    def load_yaml_file(self, path: Path) -> Dict[str, Any]:
        """Load a YAML file, returning empty dict if not found."""
        if not path.exists():
            return {}
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    
    def merge_values_files(
        self, 
        base_path: Path, 
        local_path: Path, 
        remote_path: Path
    ) -> tuple[Any, list[MergeConflict]]:
        """Merge three versions of values.yaml files preserving structure."""
        merger = PreservingYAMLMerger()
        
        base_values = merger.load_yaml_preserving(base_path)
        local_values = merger.load_yaml_preserving(local_path)
        remote_values = merger.load_yaml_preserving(remote_path)
        
        merged_values, conflicts = merger.merge(base_values, local_values, remote_values)
        
        return merged_values, conflicts 

    def merge_helm_template(
        self,
        base_path: Path,
        local_path: Path,
        remote_path: Path
    ) -> tuple[str, list[MergeConflict]]:
        """Merge Helm template files."""
        base_content = base_path.read_text() if base_path.exists() else ""
        local_content = local_path.read_text() if local_path.exists() else ""
        remote_content = remote_path.read_text() if remote_path.exists() else ""
        
        return HelmTemplateMerger.merge_template(base_content, local_content, remote_content)
    
    def apply_merged_files(
        self,
        element_path: Path,
        merged_values: Any,
        values_conflicts: list[MergeConflict],
        converter_merges: Dict[str, tuple[str, list[MergeConflict]]],
        remote_version_path: Path,
        local_additions: list[Path]
    ):
        """Apply the merged files to the element directory."""
        # Create backup first
        backup_path = element_path.parent / f".{element_path.name}.backup"
        if backup_path.exists():
            shutil.rmtree(backup_path)
        shutil.copytree(element_path, backup_path)
        
        try:
            # Apply conflict resolutions to merged values
            if values_conflicts:
                # Apply resolutions that were made
                merged_values = ConflictMarkerGenerator.apply_resolution_to_values(
                    merged_values, values_conflicts, self.yaml
                )
            
            # Write merged values.yaml preserving formatting
            values_path = element_path / "values.yaml"
            with open(values_path, 'w') as f:
                self.yaml.dump(merged_values, f)
            
            # If there are unresolved conflicts, add them as comments
            if any(not c.resolution for c in values_conflicts):
                # Add conflict information as YAML comments at the top
                with open(values_path, 'r') as f:
                    content = f.read()
                
                conflict_header = [
                    "# MERGE CONFLICTS FOUND",
                    f"# Upgrade from {baseline.version} to {target_version}",
                    "# The following paths had conflicts:",
                ]
                
                for conflict in values_conflicts:
                    if not conflict.resolution:
                        conflict_header.append(f"#   - {conflict.path}")
                        conflict_header.append(f"#     Local: {conflict.local_value}")
                        conflict_header.append(f"#     Remote: {conflict.remote_value}")
                
                conflict_header.append("# Please review and resolve manually")
                conflict_header.append("")
                
                with open(values_path, 'w') as f:
                    f.write('\n'.join(conflict_header))
                    f.write(content) 

            # Process converters
            converters_dir = element_path / "converters"
            converters_dir.mkdir(exist_ok=True)
            
            for converter_name, (content, conflicts) in converter_merges.items():
                converter_path = converters_dir / converter_name
                if conflicts:
                    ConflictMarkerGenerator.write_conflicted_file(
                        converter_path, content, conflicts
                    )
                else:
                    converter_path.write_text(content)
            
            # Copy new converter files that don't exist locally
            remote_converters = remote_version_path / "converters"
            if remote_converters.exists():
                for remote_converter in remote_converters.glob("*.yaml"):
                    local_converter = converters_dir / remote_converter.name
                    if not local_converter.exists():
                        shutil.copy2(remote_converter, local_converter)
            
            # Update Chart.yaml from remote (preserving name)
            remote_chart = remote_version_path / "Chart.yaml"
            local_chart = element_path / "Chart.yaml"
            if remote_chart.exists():
                with open(remote_chart, 'r') as f:
                    chart_data = yaml.safe_load(f)
                
                # Preserve local element name
                with open(local_chart, 'r') as f:
                    local_chart_data = yaml.safe_load(f)
                    chart_data['name'] = local_chart_data.get('name', element_path.name)
                
                with open(local_chart, 'w') as f:
                    yaml.dump(chart_data, f, default_flow_style=False, sort_keys=False)
            
            # Copy templates directory
            remote_templates = remote_version_path / "templates"
            local_templates = element_path / "templates"
            if remote_templates.exists():
                if local_templates.exists():
                    shutil.rmtree(local_templates)
                shutil.copytree(remote_templates, local_templates)
            
            # Update examples from remote
            remote_examples = remote_version_path / "examples"
            local_examples = element_path / "examples"
            if remote_examples.exists():
                if local_examples.exists():
                    shutil.rmtree(local_examples)
                shutil.copytree(remote_examples, local_examples)
            
            # Remove backup if successful
            shutil.rmtree(backup_path)
            
        except Exception as e:
            # Restore from backup on failure
            self.console.print(f"[red]Error during upgrade, restoring backup...[/red]")
            shutil.rmtree(element_path)
            shutil.move(backup_path, element_path)
            raise e


@click.command()
@click.argument("element_name")
@click.option(
    "--to-version",
    "-v",
    help="Target version to upgrade to (defaults to latest)",
)
@click.option(
    "--path",
    "-p",
    type=click.Path(exists=True, path_type=Path),
    help="PolicyStack project path",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force upgrade even if not on upgrade path",
)
@click.option(
    "--auto-resolve",
    is_flag=True,
    help="Automatically resolve conflicts where possible",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be upgraded without making changes",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompts",
)
@click.pass_obj
def upgrade(
    ctx,
    element_name: str,
    to_version: Optional[str],
    path: Optional[Path],
    force: bool,
    auto_resolve: bool,
    dry_run: bool,
    yes: bool,
) -> None:
    """
    Upgrade an installed template to a newer version.
    
    Intelligently merges local changes with new template version,
    detecting conflicts and providing resolution options.
    
    Examples:
    
    \b
        # Upgrade to latest version
        policystack upgrade openshift-logging
        
    \b
        # Upgrade to specific version
        policystack upgrade openshift-logging --to-version 1.2.0
        
    \b
        # Dry run to preview changes
        policystack upgrade openshift-logging --dry-run
        
    \b
        # Auto-resolve conflicts where possible
        policystack upgrade openshift-logging --auto-resolve
    """
    console: Console = ctx.console
    marketplace = ctx.marketplace
    
    # Determine stack path
    if path:
        stack_path = path / "stack"
    else:
        stack_path = Path.cwd() / "stack"
    
    element_path = stack_path / element_name
    
    if not element_path.exists():
        console.print(f"[red]Element '{element_name}' not found at {element_path}[/red]")
        return
    
    # Initialize change detector
    detector = ChangeDetector(element_path)
    
    # Load current version from snapshot
    snapshot_path = element_path / '.policystack' / 'snapshots' / 'baseline.json'
    if not snapshot_path.exists():
        console.print("[red]Cannot determine current version. Was this template properly installed?[/red]")
        console.print("\n[yellow]Creating baseline snapshot from current state...[/yellow]")
        
        # Try to determine version from Chart.yaml
        chart_path = element_path / "Chart.yaml"
        if chart_path.exists():
            with open(chart_path, 'r') as f:
                chart = yaml.safe_load(f)
                version = chart.get('appVersion', '0.0.0')
        else:
            version = "unknown"
        
        baseline = detector.capture_baseline(version)
        console.print(f"[green]✓ Created baseline snapshot (version: {version})[/green]")
    else:
        baseline = TemplateSnapshot.load(snapshot_path)
    
    current_version = baseline.version
    
    async def get_and_upgrade():
        # Get template metadata
        template = await marketplace.get_template(element_name)
        if not template:
            raise ValueError(f"Template '{element_name}' not found in marketplace")
        
        # Determine target version
        target_version = to_version or template.latest_version
        
        # Get version details
        target_version_details = template.metadata.get_version_details(target_version)
        if not target_version_details:
            raise ValueError(f"Version {target_version} not found")
        
        # Check upgrade path
        if hasattr(target_version_details, 'can_upgrade_from'):
            can_upgrade, reason = target_version_details.can_upgrade_from(current_version)
        else:
            # If using base model without upgrade paths, allow all upgrades
            can_upgrade, reason = True, "No upgrade constraints defined"
        
        if not can_upgrade and not force:
            raise ValueError(f"Cannot upgrade: {reason}\nUse --force to override")
        
        # Detect local changes
        console.print("\n[bold]Analyzing local changes...[/bold]")
        changes = detector.detect_changes()
        values_changes = detector.detect_values_changes()
        
        # Display upgrade plan
        console.print(
            Panel.fit(
                f"[bold]Upgrade Plan[/bold]\n"
                f"Element: {element_name}\n"
                f"Current: {current_version} → Target: {target_version}\n"
                f"Local modifications: {len(changes['modified'])} files\n"
                f"Local additions: {len(changes['added'])} files\n"
                f"Deletions: {len(changes['deleted'])} files",
                border_style="cyan"
            )
        )
        
        if dry_run:
            console.print("\n[yellow]DRY RUN MODE - No changes will be made[/yellow]")
            
            if changes['modified']:
                console.print("\n[bold]Files with local changes to merge:[/bold]")
                for change in changes['modified']:
                    rel_path = change.path.relative_to(element_path)
                    console.print(f"  • {rel_path}")
                    if 'values.yaml' in str(rel_path):
                        console.print(f"    [dim]Values changes: {len(values_changes.get('changed', {}))} fields[/dim]")
            
            if changes['added']:
                console.print("\n[bold]Local files to preserve:[/bold]")
                for change in changes['added']:
                    console.print(f"  • {change.path.relative_to(element_path)}")
            
            if changes['deleted']:
                console.print("\n[bold]Deleted files:[/bold]")
                for change in changes['deleted']:
                    console.print(f"  • {change.path.relative_to(element_path)}")
            
            return
        
        # Confirm upgrade
        if not yes:
            if not Confirm.ask("\n[bold]Proceed with upgrade?[/bold]", default=True):
                console.print("[yellow]Upgrade cancelled[/yellow]")
                return
        
        # Perform upgrade
        upgrader = TemplateUpgrader(marketplace, console)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Performing upgrade...", total=5)
            
            # Step 1: Download new version
            progress.update(task, description=f"Downloading version {target_version}...")
            remote_version_path = await upgrader.download_version(
                element_name, target_version, template.repository
            )
            progress.advance(task)
            
            # Step 2: Download base version for three-way merge
            progress.update(task, description=f"Downloading base version {current_version}...")
            try:
                base_version_path = await upgrader.download_version(
                    element_name, current_version, template.repository
                )
            except:
                # If can't get base version, use current as base
                console.print("[yellow]Cannot download base version, using current state as base[/yellow]")
                base_version_path = element_path
            progress.advance(task)
            
            # Step 3: Perform three-way merge
            progress.update(task, description="Merging changes...")
            
            conflict_report = ConflictReport(current_version, target_version, element_name)
            
            # Merge values.yaml
            merged_values, values_conflicts = upgrader.merge_values_files(
                base_version_path / "values.yaml",
                element_path / "values.yaml",
                remote_version_path / "values.yaml"
            )
            
            for conflict in values_conflicts:
                conflict_report.add_conflict("values.yaml", conflict)
            
            # Merge converters
            converter_merges = {}
            converters_dir = element_path / "converters"
            if converters_dir.exists():
                for local_converter in converters_dir.glob("*.yaml"):
                    converter_name = local_converter.name
                    
                    base_converter = base_version_path / "converters" / converter_name
                    remote_converter = remote_version_path / "converters" / converter_name
                    
                    if remote_converter.exists():
                        merged_content, conflicts = upgrader.merge_helm_template(
                            base_converter,
                            local_converter,
                            remote_converter
                        )
                        converter_merges[converter_name] = (merged_content, conflicts)
                        
                        for conflict in conflicts:
                            conflict_report.add_conflict(f"converters/{converter_name}", conflict)
                    else:
                        # File removed in remote, keep local version
                        converter_merges[converter_name] = (local_converter.read_text(), [])
            
            progress.advance(task)
            
            # Step 4: Handle conflicts
            if conflict_report.has_conflicts():
                progress.update(task, description="Resolving conflicts...")
                
                if auto_resolve:
                    # Auto-resolve what is possible
                    auto_resolved = 0
                    for category_conflicts in conflict_report.conflicts.values():
                        for conflict in category_conflicts:
                            if conflict.auto_resolvable:
                                auto_resolved += 1
                    
                    if auto_resolved > 0:
                        console.print(f"\n[green]✓ Auto-resolved {auto_resolved} conflicts[/green]")
                    
                    # Handle remaining interactively
                    remaining = conflict_report.get_unresolved_count()
                    if remaining > 0:
                        progress.stop()
                        resolver = ConflictResolver(console)
                        conflict_report = resolver.resolve_interactively(conflict_report)
                        progress.start()
                else:
                    # Interactive resolution
                    progress.stop()
                    resolver = ConflictResolver(console)
                    conflict_report = resolver.resolve_interactively(conflict_report)
                    progress.start()
                
                # Save conflict report
                report_path = element_path / '.policystack' / 'upgrade'
                report_path.mkdir(parents=True, exist_ok=True)
                conflict_report.save(report_path / 'conflicts.yaml')
            
            progress.advance(task)
            
            # Step 5: Apply upgrade
            progress.update(task, description="Applying upgrade...")
            
            upgrader.apply_merged_files(
                element_path,
                merged_values,
                values_conflicts if not auto_resolve else [],
                converter_merges,
                remote_version_path,
                [c.path for c in changes['added']]
            )
            
            # Update baseline snapshot
            new_snapshot = TemplateSnapshot(element_path, target_version)
            new_snapshot.save(snapshot_path)
            
            # Cleanup temp directories
            if remote_version_path != element_path:
                shutil.rmtree(remote_version_path)
            if base_version_path != element_path:
                shutil.rmtree(base_version_path)
            
            progress.advance(task)
        
        console.print(f"\n[green]✓ Successfully upgraded {element_name} to {target_version}[/green]")
        
        if conflict_report.has_conflicts():
            resolved = len([c for cat in conflict_report.conflicts.values() 
                          for c in cat if c.resolution])
            console.print(
                f"\n[yellow]ℹ {resolved} conflicts were resolved. "
                f"Review the changes in {element_path}[/yellow]"
            )
            
            if any('CONFLICT' in str(f.read_text()) 
                   for f in element_path.rglob('*.yaml') if f.is_file()):
                console.print(
                    "[yellow]⚠ Some files contain conflict markers. "
                    "Search for 'MERGE CONFLICT' to resolve them.[/yellow]"
                )
        
        # Show post-upgrade instructions
        if hasattr(target_version_details, 'upgrade') and target_version_details.upgrade:
            if target_version_details.upgrade.post_upgrade_hook:
                console.print(
                    f"\n[bold]Post-upgrade steps required:[/bold]\n"
                    f"Run: {target_version_details.upgrade.post_upgrade_hook}"
                )
    
    try:
        asyncio.run(get_and_upgrade())
    except Exception as e:
        console.print(f"[red]Upgrade failed: {e}[/red]")
        if ctx.debug:
            console.print_exception()
