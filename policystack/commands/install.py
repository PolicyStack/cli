"""Install command for PolicyStack CLI."""

import asyncio
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm

from ..core.installer import TemplateInstaller
from ..models import Template


@click.command()
@click.argument("template_name")
@click.option(
    "--version",
    "-v",
    help="Template version to install (defaults to latest)",
)
@click.option(
    "--path",
    "-p",
    type=click.Path(path_type=Path),
    help="PolicyStack project path (defaults to current directory)",
)
@click.option(
    "--element-name",
    "-n",
    help="Custom element name (defaults to template name)",
)
@click.option(
    "--repository",
    "-r",
    help="Specify repository to install from",
)
@click.option(
    "--example",
    "-e",
    type=click.Choice(["minimal", "production", "advanced", "custom"]),
    help="Use example configuration as base",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force installation even if element exists",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be installed without making changes",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompts",
)
@click.pass_obj
def install(
    ctx,
    template_name: str,
    version: Optional[str],
    path: Optional[Path],
    element_name: Optional[str],
    repository: Optional[str],
    example: Optional[str],
    force: bool,
    dry_run: bool,
    yes: bool,
) -> None:
    """
    Install a template into your PolicyStack project.

    Downloads and installs a template from the marketplace into your
    PolicyStack stack directory. The template will be configured as a
    new element ready for customization.

    Examples:

    \b
        # Install latest version to current directory
        policystack install openshift-logging

    \b
        # Install specific version
        policystack install openshift-logging --version 1.0.0

    \b
        # Install to specific path with custom name
        policystack install openshift-logging -p ~/my-stack -n my-logging

    \b
        # Install with production example config
        policystack install openshift-logging --example production

    \b
        # Dry run to see what would be installed
        policystack install openshift-logging --dry-run
    """
    console: Console = ctx.console
    marketplace = ctx.marketplace

    # Determine installation path
    if path:
        stack_path = path
    else:
        # Look for stack directory in current path or use config default
        if (Path.cwd() / "stack").exists():
            stack_path = Path.cwd()
        else:
            stack_path = ctx.config.config.default_stack_path.parent

    stack_dir = stack_path / "stack"

    # Validate stack directory
    if not dry_run and not stack_dir.exists():
        if not yes:
            create = Confirm.ask(
                f"Stack directory not found at {stack_dir}. Create it?",
                default=True,
            )
            if not create:
                console.print("[yellow]Installation cancelled[/yellow]")
                return
        stack_dir.mkdir(parents=True, exist_ok=True)

    async def get_template() -> Optional[Template]:
        """Get template from marketplace."""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            progress.add_task("Fetching template information...", total=None)

            template = await marketplace.get_template(template_name, repository)

            if not template:
                # Try searching for partial matches
                results = await marketplace.search(template_name)
                if results:
                    console.print(f"[yellow]Template '{template_name}' not found.[/yellow]")
                    console.print("\nDid you mean one of these?")
                    for result in results[:5]:
                        console.print(f"  • {result.name} ({result.repository})")
                else:
                    console.print(f"[red]Template '{template_name}' not found[/red]")
                return None

            return template

    # Get template
    try:
        template = asyncio.run(get_template())
        if not template:
            return
    except Exception as e:
        console.print(f"[red]Failed to get template: {e}[/red]")
        if ctx.debug:
            console.print_exception()
        return

    # Determine version to install
    install_version = version or template.latest_version

    # Validate version
    if install_version not in template.metadata.version.supported:
        if install_version in template.metadata.version.deprecated:
            console.print(f"[yellow]Warning: Version {install_version} is deprecated[/yellow]")
            if not yes:
                proceed = Confirm.ask("Continue with deprecated version?", default=False)
                if not proceed:
                    return
        else:
            console.print(f"[red]Version {install_version} not found[/red]")
            console.print(f"Available versions: {', '.join(template.metadata.version.supported)}")
            return

    # Determine element name
    final_element_name = element_name or template.name
    element_path = stack_dir / final_element_name

    # Check if element exists
    if element_path.exists() and not force:
        console.print(
            f"[yellow]Element '{final_element_name}' already exists at {element_path}[/yellow]"
        )

        if not yes:
            overwrite = Confirm.ask("Overwrite existing element?", default=False)
            if not overwrite:
                console.print("[yellow]Installation cancelled[/yellow]")
                return

    # Display installation plan
    console.print(
        Panel.fit(
            f"[bold]Template:[/bold] {template.display_name}\n"
            f"[bold]Version:[/bold] {install_version}\n"
            f"[bold]Repository:[/bold] {template.repository}\n"
            f"[bold]Element Name:[/bold] {final_element_name}\n"
            f"[bold]Install Path:[/bold] {element_path}",
            title="Installation Plan",
            border_style="green",
        )
    )

    # Get version details
    version_details = template.metadata.get_version_details(install_version)
    if version_details:
        # Show requirements
        console.print("\n[bold]Requirements:[/bold]")
        console.print(f"  • PolicyStack Library: {version_details.policy_library}")
        console.print(f"  • OpenShift: {version_details.openshift}")
        console.print(f"  • ACM: {version_details.acm}")

        # Show breaking changes warning
        if version_details.breaking:
            console.print("\n[bold red]⚠ This version contains breaking changes![/bold red]")
            if version_details.migration:
                console.print(version_details.migration)

    # Dry run mode
    if dry_run:
        console.print("\n[yellow]DRY RUN MODE - No changes will be made[/yellow]")
        console.print("\nWould perform the following actions:")
        console.print(f"  1. Create directory: {element_path}")
        console.print(f"  2. Download template version {install_version}")
        console.print(f"  3. Copy Chart.yaml")
        console.print(f"  4. Copy values.yaml" + (f" (using {example} example)" if example else ""))
        console.print(f"  5. Copy converters/")
        console.print(f"  6. Create .gitignore")
        console.print(f"  7. Update values with element name")
        return

    # Confirm installation
    if not yes:
        proceed = Confirm.ask("\n[bold]Proceed with installation?[/bold]", default=True)
        if not proceed:
            console.print("[yellow]Installation cancelled[/yellow]")
            return

    # Perform installation
    try:
        installer = TemplateInstaller(
            marketplace=marketplace,
            console=console,
        )

        success = asyncio.run(
            installer.install(
                template=template,
                version=install_version,
                element_name=final_element_name,
                stack_dir=stack_dir,
                example_name=example,
                force=force,
            )
        )

        if success:
            console.print(
                f"\n[green]✓ Successfully installed {template.name} as '{final_element_name}'[/green]"
            )

            # Show next steps
            console.print("\n[bold]Next Steps:[/bold]")
            console.print(f"  1. Navigate to: {element_path}")
            console.print(f"  2. Edit values.yaml to customize the configuration")
            console.print(f"  3. Set 'enable: true' in values.yaml to activate")
            console.print(f"  4. Deploy using your PolicyStack workflow")

            # Show example customization
            if example:
                console.print(f"\n[dim]Note: Using {example} example as base configuration[/dim]")
            else:
                console.print(
                    f"\n[dim]Tip: Check {element_path}/examples/ for configuration examples[/dim]"
                )
        else:
            console.print(f"[red]Installation failed[/red]")

    except Exception as e:
        console.print(f"[red]Installation error: {e}[/red]")
        if ctx.debug:
            console.print_exception()
