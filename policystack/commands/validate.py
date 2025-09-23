"""Validate command for PolicyStack CLI - Validate template structure."""

import json
from pathlib import Path
from typing import List

import click
import yaml
from packaging import version as pkg_version
from rich.console import Console
from rich.panel import Panel
from rich.table import Table


class TemplateValidator:
    """Validates template structure and metadata."""

    def __init__(self, template_path: Path, console: Console):
        """Initialize validator."""
        self.template_path = template_path
        self.console = console
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.info: List[str] = []

    def validate(self) -> bool:
        """Run all validation checks."""
        self.console.print(f"🔍 Validating template: [bold]{self.template_path.name}[/bold]\n")

        # Check directory structure
        self.validate_structure()

        # Validate metadata.yaml
        if (self.template_path / "metadata.yaml").exists():
            self.validate_metadata()

        # Validate versions
        self.validate_versions()

        # Validate examples
        self.validate_examples()

        # Print results
        self.print_results()

        return len(self.errors) == 0

    def validate_structure(self) -> None:
        """Validate directory structure."""
        required_files = ["metadata.yaml", "README.md"]
        required_dirs = ["versions"]  # examples is optional

        for file in required_files:
            if not (self.template_path / file).exists():
                self.errors.append(f"Missing required file: {file}")
            else:
                self.info.append(f"✓ Found {file}")

        for dir_name in required_dirs:
            if not (self.template_path / dir_name).exists():
                self.errors.append(f"Missing required directory: {dir_name}")
            else:
                self.info.append(f"✓ Found {dir_name}/")

        # Check for examples (recommended but not required)
        if not (self.template_path / "examples").exists():
            self.warnings.append("No examples directory found (recommended)")

    def validate_metadata(self) -> None:
        """Validate metadata.yaml content."""
        metadata_file = self.template_path / "metadata.yaml"

        try:
            with open(metadata_file, "r") as f:
                metadata = yaml.safe_load(f)

            # Required fields
            required_fields = [
                "name",
                "displayName",
                "description",
                "author",
                "categories",
                "version",
                "versions",
            ]

            for field in required_fields:
                if field not in metadata:
                    self.errors.append(f"metadata.yaml missing required field: {field}")
                else:
                    self.info.append(f"✓ Has {field}")

            # Validate author
            if "author" in metadata:
                if not isinstance(metadata["author"], dict):
                    self.errors.append("author must be a dictionary")
                elif "name" not in metadata["author"]:
                    self.errors.append("author must have a name field")

            # Validate categories
            if "categories" in metadata:
                if "primary" not in metadata["categories"]:
                    self.errors.append("categories must have a primary category")

            # Validate version info
            if "version" in metadata:
                latest = metadata["version"].get("latest")
                if not latest:
                    self.errors.append("version must specify latest")
                else:
                    # Validate version format
                    try:
                        pkg_version.parse(latest)
                        self.info.append(f"✓ Latest version: {latest}")
                    except pkg_version.InvalidVersion:
                        self.errors.append(f"Invalid version format: {latest}")

                # Check that latest version exists in versions
                if "versions" in metadata and latest not in metadata["versions"]:
                    self.errors.append(f"Latest version {latest} not found in versions")

            # Validate each version entry
            if "versions" in metadata:
                for version_num, details in metadata["versions"].items():
                    if not isinstance(details, dict):
                        self.errors.append(f"Version {version_num} must have details")
                        continue

                    recommended_fields = [
                        "date",
                        "policyLibrary",
                        "openshift",
                        "acm",
                        "changes",
                    ]
                    for field in recommended_fields:
                        if field not in details:
                            self.warnings.append(f"Version {version_num} missing field: {field}")

            # Check for recommended fields
            recommended_fields = ["tags", "requirements"]
            for field in recommended_fields:
                if field not in metadata:
                    self.warnings.append(f"Consider adding recommended field: {field}")

            # Store template info
            self.info.append(f"Template name: {metadata.get('name')}")
            self.info.append(f"Display name: {metadata.get('displayName')}")
            self.info.append(f"Total versions: {len(metadata.get('versions', {}))}")

        except yaml.YAMLError as e:
            self.errors.append(f"Invalid YAML in metadata.yaml: {e}")
        except Exception as e:
            self.errors.append(f"Error reading metadata.yaml: {e}")

    def validate_versions(self) -> None:
        """Validate version directories."""
        versions_dir = self.template_path / "versions"

        if not versions_dir.exists():
            return

        version_dirs = [d for d in versions_dir.iterdir() if d.is_dir()]

        if not version_dirs:
            self.errors.append("No version directories found in versions/")
            return

        for version_dir in version_dirs:
            version_name = version_dir.name

            # Validate version format
            try:
                pkg_version.parse(version_name)
            except pkg_version.InvalidVersion:
                self.errors.append(f"Invalid version directory name: {version_name}")
                continue

            # Check for required files in each version
            required_files = ["Chart.yaml", "values.yaml"]
            for file in required_files:
                if not (version_dir / file).exists():
                    self.errors.append(f"Version {version_name} missing required file: {file}")
                else:
                    self.info.append(f"✓ {version_name}/{file}")

            # Validate Chart.yaml
            chart_file = version_dir / "Chart.yaml"
            if chart_file.exists():
                try:
                    with open(chart_file, "r") as f:
                        chart = yaml.safe_load(f)

                    # Check for required fields
                    if "name" not in chart:
                        self.errors.append(f"Version {version_name}: Chart.yaml missing name")

                    if "dependencies" not in chart:
                        self.warnings.append(
                            f"Version {version_name}: Chart.yaml missing dependencies"
                        )
                    else:
                        # Check for policy-library dependency
                        has_policy_lib = any(
                            dep.get("name") == "policy-library"
                            for dep in chart.get("dependencies", [])
                        )
                        if not has_policy_lib:
                            self.errors.append(
                                f"Version {version_name}: Missing policy-library dependency"
                            )

                except Exception as e:
                    self.errors.append(f"Version {version_name}: Invalid Chart.yaml: {e}")

            # Validate values.yaml
            values_file = version_dir / "values.yaml"
            if values_file.exists():
                try:
                    with open(values_file, "r") as f:
                        values = yaml.safe_load(f)

                    # Check for stack structure
                    if "stack" not in values:
                        self.errors.append(
                            f"Version {version_name}: values.yaml missing 'stack' key"
                        )

                except Exception as e:
                    self.errors.append(f"Version {version_name}: Invalid values.yaml: {e}")

            # Check for converters directory
            if not (version_dir / "converters").exists():
                self.warnings.append(f"Version {version_name}: No converters directory")

            # Check for templates directory
            if not (version_dir / "templates").exists():
                self.errors.append(f"Version {version_name}: Missing templates directory")
            elif not (version_dir / "templates" / "policy.yaml").exists():
                self.warnings.append(f"Version {version_name}: No policy.yaml in templates/")

    def validate_examples(self) -> None:
        """Validate example files."""
        examples_dir = self.template_path / "examples"

        if not examples_dir.exists():
            return

        example_files = list(examples_dir.glob("*.yaml")) + list(examples_dir.glob("*.yml"))

        if not example_files:
            self.warnings.append("Examples directory exists but contains no YAML files")
            return

        for example_file in example_files:
            try:
                with open(example_file, "r") as f:
                    example = yaml.safe_load(f)

                # Basic validation - check if it has the stack structure
                if not isinstance(example, dict):
                    self.errors.append(f"Example {example_file.name}: Not a valid YAML dictionary")
                elif "stack" not in example:
                    self.warnings.append(f"Example {example_file.name}: Missing 'stack' key")
                else:
                    self.info.append(f"✓ Valid example: {example_file.name}")

            except yaml.YAMLError as e:
                self.errors.append(f"Example {example_file.name}: Invalid YAML: {e}")
            except Exception as e:
                self.errors.append(f"Example {example_file.name}: Error reading file: {e}")

    def print_results(self) -> None:
        """Print validation results."""
        # Create results table
        if self.errors or self.warnings:
            table = Table(title="Validation Results", show_header=True, header_style="bold")
            table.add_column("Type", style="bold", width=10)
            table.add_column("Message")

            for error in self.errors:
                table.add_row("[red]ERROR[/red]", error)

            for warning in self.warnings:
                table.add_row("[yellow]WARNING[/yellow]", warning)

            self.console.print(table)
            self.console.print()

        # Summary
        if self.errors:
            self.console.print(
                Panel.fit(
                    f"[red]✗ Validation FAILED[/red]\n"
                    f"{len(self.errors)} error(s), {len(self.warnings)} warning(s)",
                    border_style="red",
                )
            )
        else:
            message = "[green]✓ Validation PASSED[/green]"
            if self.warnings:
                message += f"\n{len(self.warnings)} warning(s)"
            self.console.print(
                Panel.fit(
                    message,
                    border_style="green" if not self.warnings else "yellow",
                )
            )


@click.command()
@click.argument("template_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output results as JSON",
)
@click.option(
    "--strict",
    is_flag=True,
    help="Treat warnings as errors",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Only show errors and final result",
)
@click.pass_obj
def validate(
    ctx,
    template_path: Path,
    output_json: bool,
    strict: bool,
    quiet: bool,
) -> None:
    """
    Validate a PolicyStack template.

    Checks template structure, metadata, versions, and examples for
    correctness and completeness.

    Examples:

    \b
        # Validate a template by name (in templates/ directory)
        policystack validate openshift-logging

    \b
        # Validate a template at specific path
        policystack validate /path/to/template

    \b
        # Validate with strict mode (warnings as errors)
        policystack validate openshift-logging --strict

    \b
        # Get JSON output for CI/CD
        policystack validate openshift-logging --json
    """
    console: Console = ctx.console

    # Resolve template path
    if not template_path.is_absolute():
        # Check if it's a template name in templates/ directory
        if (Path.cwd() / "templates" / template_path).exists():
            template_path = Path.cwd() / "templates" / template_path
        elif (Path.cwd() / template_path).exists():
            template_path = Path.cwd() / template_path

    if not template_path.exists():
        console.print(f"[red]Template path not found: {template_path}[/red]")
        ctx.exit(1)

    if not template_path.is_dir():
        console.print(f"[red]Template path must be a directory: {template_path}[/red]")
        ctx.exit(1)

    # Run validation
    validator = TemplateValidator(template_path, console)

    if not quiet:
        validator.validate()
    else:
        # Suppress info messages in quiet mode
        original_info = validator.info
        validator.info = []
        validator.validate()
        validator.info = original_info

    # Output JSON if requested
    if output_json:
        results = {
            "template": str(template_path),
            "valid": len(validator.errors) == 0 and (not strict or len(validator.warnings) == 0),
            "errors": validator.errors,
            "warnings": validator.warnings,
            "info": validator.info,
            "counts": {
                "errors": len(validator.errors),
                "warnings": len(validator.warnings),
                "info": len(validator.info),
            },
        }
        console.print(json.dumps(results, indent=2))
    elif quiet:
        # In quiet mode, only show final result
        if validator.errors or (strict and validator.warnings):
            console.print(f"[red]✗ Validation failed[/red]")
        else:
            console.print(f"[green]✓ Validation passed[/green]")

    # Exit with appropriate code
    if validator.errors:
        ctx.exit(1)
    elif strict and validator.warnings:
        ctx.exit(1)
    else:
        ctx.exit(0)
