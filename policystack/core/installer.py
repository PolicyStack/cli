"""Template installer for PolicyStack CLI."""

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Optional

import httpx
import yaml
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

from ..models import Template
from ..core.change_detector import ChangeDetector
from .git_repository import GitRepositoryHandler

logger = logging.getLogger(__name__)


class TemplateInstaller:
    """Handles template installation to PolicyStack projects."""

    def __init__(self, marketplace: Any, console: Console) -> None:
        """Initialize installer."""
        self.marketplace = marketplace
        self.console = console
        self.git_handler = GitRepositoryHandler(
            self.marketplace.cache_dir
            if hasattr(self.marketplace, "cache_dir")
            else Path.home() / ".policystack" / "cache"
        )

    async def install(
        self,
        template: Template,
        version: str,
        element_name: str,
        stack_dir: Path,
        example_name: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        """
        Install a template as an element in the PolicyStack.

        Args:
            template: Template to install
            version: Version to install
            element_name: Name for the element
            stack_dir: Path to stack directory
            example_name: Example configuration to use
            force: Force overwrite if exists

        Returns:
            True if installation successful
        """
        element_path = stack_dir / element_name

        # Create element directory
        if element_path.exists():
            if force:
                logger.info(f"Removing existing element: {element_path}")
                shutil.rmtree(element_path)
            else:
                logger.error(f"Element already exists: {element_path}")
                return False

        element_path.mkdir(parents=True, exist_ok=True)

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                console=self.console,
            ) as progress:
                # Download template files
                task = progress.add_task("Downloading template...", total=4)

                # Get repository info
                repo = self.marketplace.get_repository(template.repository)
                if not repo:
                    raise ValueError(f"Repository {template.repository} not found")

                # Download template version
                progress.update(task, description="Fetching template files...")
                template_files = await self._download_template(template, version, repo)
                progress.advance(task)

                # Copy Chart.yaml
                progress.update(task, description="Installing Chart.yaml...")
                self._install_chart(template_files, element_path, element_name)
                progress.advance(task)

                # Copy values.yaml
                progress.update(task, description="Installing values.yaml...")
                self._install_values(template_files, element_path, element_name, example_name)
                progress.advance(task)

                # Copy converters
                progress.update(task, description="Installing converters...")
                self._install_converters(template_files, element_path)
                progress.advance(task)

                # Create .gitignore if needed
                self._create_gitignore(element_path)

                # Copy examples directory for reference
                self._install_examples(template_files, element_path)

                detector = ChangeDetector(element_path)
                baseline = detector.capture_baseline(version)
                progress.update(task, description="Installation complete!", completed=4)

            return True

        except Exception as e:
            logger.error(f"Installation failed: {e}")
            # Clean up on failure
            if element_path.exists():
                shutil.rmtree(element_path)
            raise

    async def _download_template(self, template: Template, version: str, repository: Any) -> Path:
        """Download template files from repository."""
        # Create temp directory
        temp_dir = Path(tempfile.mkdtemp(prefix="policystack_"))

        try:
            if repository.is_git:
                await self._download_git_template(template, version, repository, temp_dir)
            elif repository.is_local:
                self._copy_local_template(template, version, repository, temp_dir)
            elif repository.is_http:
                await self._download_http_template(template, version, repository, temp_dir)
            else:
                raise ValueError(f"Unsupported repository type: {repository.type}")

            return temp_dir

        except Exception:
            # Clean up temp dir on failure
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise

    async def _download_git_template(
        self, template: Template, version: str, repository: Any, temp_dir: Path
    ) -> None:
        """Download template from git repository using Git."""
        # Get template files using GitRepositoryHandler
        success, template_dir, message = self.git_handler.get_template_files(
            url=repository.url,
            template_path=template.path,
            version=version,
            branch=repository.branch,
            auth_token=None,  # Could be retrieved from config
        )

        if not success or not template_dir:
            raise Exception(f"Failed to get template files: {message}")

        # Copy files to temp_dir
        try:
            shutil.copytree(template_dir, temp_dir, dirs_exist_ok=True)
        finally:
            # Clean up the temporary template directory
            if template_dir and template_dir.exists():
                shutil.rmtree(template_dir)

    def _copy_local_template(
        self, template: Template, version: str, repository: Any, temp_dir: Path
    ) -> None:
        """Copy template from local repository."""
        repo_path = Path(repository.url).expanduser()
        template_path = repo_path / template.path / "versions" / version

        if not template_path.exists():
            raise ValueError(f"Template version not found: {template_path}")

        # Copy files
        shutil.copytree(template_path, temp_dir, dirs_exist_ok=True)

        # Copy examples
        examples_src = repo_path / template.path / "examples"
        if examples_src.exists():
            examples_dst = temp_dir / "examples"
            shutil.copytree(examples_src, examples_dst, dirs_exist_ok=True)

    async def _download_http_template(
        self, template: Template, version: str, repository: Any, temp_dir: Path
    ) -> None:
        """Download template from HTTP repository."""
        # This would download a zip/tar archive and extract it
        # Implementation depends on how the HTTP repository serves templates
        raise NotImplementedError("HTTP repository support not yet implemented")

    async def _download_file(self, client: httpx.AsyncClient, url: str, path: Path) -> None:
        """Download a file from URL."""
        response = await client.get(url)
        response.raise_for_status()

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(response.content)

    def _install_chart(self, template_dir: Path, element_path: Path, element_name: str) -> None:
        """Install Chart.yaml with updated name."""
        chart_src = template_dir / "Chart.yaml"
        chart_dst = element_path / "Chart.yaml"

        if not chart_src.exists():
            raise FileNotFoundError(f"Chart.yaml not found in template")

        # Load and update Chart.yaml
        with open(chart_src, "r") as f:
            chart_data = yaml.safe_load(f)

        # Update chart name
        chart_data["name"] = element_name

        # Write updated Chart.yaml
        with open(chart_dst, "w") as f:
            yaml.dump(chart_data, f, default_flow_style=False, sort_keys=False)

    def _install_values(
        self,
        template_dir: Path,
        element_path: Path,
        element_name: str,
        example_name: Optional[str] = None,
    ) -> None:
        """Install values.yaml with updated component name."""
        # Determine source values file
        if example_name and (template_dir / "examples" / f"{example_name}.yaml").exists():
            values_src = template_dir / "examples" / f"{example_name}.yaml"
        else:
            values_src = template_dir / "values.yaml"

        values_dst = element_path / "values.yaml"

        if not values_src.exists():
            raise FileNotFoundError(f"values.yaml not found in template")

        # Read values file content
        with open(values_src, "r") as f:
            content = f.read()

        # Write updated values
        with open(values_dst, "w") as f:
            f.write(content)

    def _install_converters(self, template_dir: Path, element_path: Path) -> None:
        """Install converter templates."""
        converters_src = template_dir / "converters"
        converters_dst = element_path / "converters"

        if converters_src.exists() and any(converters_src.iterdir()):
            converters_dst.mkdir(exist_ok=True)
            for converter_file in converters_src.glob("*.yaml"):
                shutil.copy2(converter_file, converters_dst / converter_file.name)

    def _install_examples(self, template_dir: Path, element_path: Path) -> None:
        """Install examples directory for reference."""
        examples_src = template_dir / "examples"
        examples_dst = element_path / "examples"

        if examples_src.exists() and any(examples_src.iterdir()):
            examples_dst.mkdir(exist_ok=True)
            for example_file in examples_src.glob("*.yaml"):
                shutil.copy2(example_file, examples_dst / example_file.name)

    def _create_gitignore(self, element_path: Path) -> None:
        """Create .gitignore for the element."""
        gitignore_path = element_path / ".gitignore"

        if not gitignore_path.exists():
            gitignore_content = """# Helm dependencies
Chart.lock
*.tgz
charts/
"""
            gitignore_path.write_text(gitignore_content)

    def _to_camel_case(self, name: str) -> str:
        """Convert kebab-case to camelCase."""
        parts = name.split("-")
        return parts[0].lower() + "".join(p.capitalize() for p in parts[1:])
