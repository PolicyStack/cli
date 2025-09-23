"""Initialize command for PolicyStack CLI - Create new templates."""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.tree import Tree


def to_camel_case(name: str) -> str:
    """Convert kebab-case to camelCase."""
    parts = name.split("-")
    return parts[0].lower() + "".join(p.capitalize() for p in parts[1:])


def validate_template_name(name: str) -> bool:
    """Validate template name format."""
    pattern = r"^[a-z][a-z0-9-]*[a-z0-9]$"
    return bool(re.match(pattern, name)) and "--" not in name


def validate_version(version: str) -> bool:
    """Validate semantic version format."""
    pattern = r"^\d+\.\d+\.\d+$"
    return bool(re.match(pattern, version))


@click.command()
@click.option(
    "--name",
    "-n",
    help="Template name (e.g., openshift-logging)",
)
@click.option(
    "--path",
    "-p",
    type=click.Path(path_type=Path),
    help="Marketplace path (defaults to current directory)",
)
@click.option(
    "--version",
    "-v",
    help="Initial version (defaults to 1.0.0)",
)
@click.option(
    "--author",
    help="Author name",
)
@click.option(
    "--email",
    help="Author email",
)
@click.option(
    "--github",
    help="Author GitHub handle",
)
@click.option(
    "--category",
    help="Primary category",
)
@click.option(
    "--description",
    "-d",
    help="Template description",
)
@click.option(
    "--skip-examples",
    is_flag=True,
    help="Skip creating example files",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompts",
)
@click.pass_obj
def init(
    ctx,
    name: Optional[str],
    path: Optional[Path],
    version: Optional[str],
    author: Optional[str],
    email: Optional[str],
    github: Optional[str],
    category: Optional[str],
    description: Optional[str],
    skip_examples: bool,
    yes: bool,
) -> None:
    """
    Initialize a new template in the marketplace.

    Creates the directory structure, metadata, and initial version files
    for a new PolicyStack template.

    Examples:

    \b
        # Interactive initialization
        policystack init

    \b
        # Initialize with name and version
        policystack init --name my-operator --version 0.1.0

    \b
        # Full initialization
        policystack init \\
            --name openshift-monitoring \\
            --version 1.0.0 \\
            --author "John Doe" \\
            --email john@example.com \\
            --category observability
    """
    console: Console = ctx.console

    # Display welcome message
    console.print(
        Panel.fit(
            "[bold cyan]PolicyStack Template Initializer[/bold cyan]\n"
            "Create a new template for the marketplace",
            border_style="cyan",
        )
    )

    # Gather template information interactively
    template_info = {}

    # Template name
    while True:
        if name:
            template_info["name"] = name
            name = None  # Clear for next iteration if validation fails
        else:
            template_info["name"] = Prompt.ask(
                "\n[bold]Template name[/bold] (e.g., openshift-logging)",
                default=None,
            )

        if not template_info["name"]:
            console.print("[red]Template name is required[/red]")
            continue

        if not validate_template_name(template_info["name"]):
            console.print(
                "[red]Invalid name format. Use lowercase letters, numbers, and hyphens.[/red]"
            )
            console.print("[dim]Example: openshift-logging, cert-manager, aws-efs-csi[/dim]")
            continue

        break

    # Display name
    default_display = template_info["name"].replace("-", " ").title()
    template_info["display_name"] = Prompt.ask(
        "[bold]Display name[/bold]",
        default=default_display,
    )

    # Description
    if description:
        template_info["description"] = description
    else:
        template_info["description"] = Prompt.ask(
            "[bold]Description[/bold]",
            default=f"Configuration for {template_info['display_name']}",
        )

    # Version
    while True:
        if version:
            template_info["version"] = version
            version = None
        else:
            template_info["version"] = Prompt.ask(
                "[bold]Initial version[/bold]",
                default="1.0.0",
            )

        if not validate_version(template_info["version"]):
            console.print("[red]Invalid version format. Use semantic versioning (X.Y.Z)[/red]")
            continue

        break

    # Author information
    console.print("\n[bold]Author Information:[/bold]")
    template_info["author_name"] = author or Prompt.ask("  Name", default="")
    template_info["author_email"] = email or Prompt.ask("  Email", default="")
    template_info["author_github"] = github or Prompt.ask("  GitHub handle", default="")

    # Category
    categories = [
        "observability",
        "security",
        "networking",
        "storage",
        "operators",
        "compliance",
        "backup",
        "monitoring",
        "logging",
        "service-mesh",
        "ci-cd",
        "database",
        "messaging",
        "other",
    ]

    console.print("\n[bold]Categories:[/bold]")
    for i, cat in enumerate(categories, 1):
        console.print(f"  {i:2}. {cat}")

    if category and category in categories:
        template_info["category"] = category
    else:
        while True:
            cat_input = Prompt.ask("Primary category (number or name)", default="other")

            if cat_input.isdigit():
                idx = int(cat_input) - 1
                if 0 <= idx < len(categories):
                    template_info["category"] = categories[idx]
                    break
            elif cat_input in categories:
                template_info["category"] = cat_input
                break
            else:
                console.print("[red]Invalid category. Please choose from the list.[/red]")

    # Tags
    default_tags = [
        "acm-policy",
        "gitops",
        template_info["category"],
        template_info["name"],
    ]

    tags_input = Prompt.ask(
        "\n[bold]Tags[/bold] (comma-separated)",
        default=", ".join(default_tags),
    )
    template_info["tags"] = [tag.strip() for tag in tags_input.split(",") if tag.strip()]

    # Requirements
    console.print("\n[bold]Requirements:[/bold]")
    template_info["openshift_version"] = Prompt.ask(
        "  Minimum OpenShift version",
        default="4.11.0",
    )
    template_info["acm_version"] = Prompt.ask(
        "  Minimum ACM version",
        default="2.8.0",
    )
    template_info["policy_library_version"] = Prompt.ask(
        "  PolicyStack library version",
        default="1.1.0",
    )

    # Determine path
    if path:
        marketplace_path = path
    else:
        # Check if we're in a marketplace repo
        if (Path.cwd() / "templates").exists():
            marketplace_path = Path.cwd()
        else:
            marketplace_path = Path.cwd()

    template_path = marketplace_path / "templates" / template_info["name"]

    # Check if template already exists
    if template_path.exists():
        console.print(f"\n[red]Template already exists at {template_path}[/red]")
        if not yes:
            overwrite = Confirm.ask("Overwrite existing template?", default=False)
            if not overwrite:
                console.print("[yellow]Initialization cancelled[/yellow]")
                return

        import shutil

        shutil.rmtree(template_path)

    # Display summary
    console.print(
        Panel.fit(
            f"[bold]Template:[/bold] {template_info['name']}\n"
            f"[bold]Display:[/bold] {template_info['display_name']}\n"
            f"[bold]Version:[/bold] {template_info['version']}\n"
            f"[bold]Category:[/bold] {template_info['category']}\n"
            f"[bold]Path:[/bold] {template_path}",
            title="Template Summary",
            border_style="green",
        )
    )

    # Confirm
    if not yes:
        proceed = Confirm.ask("\n[bold]Create template?[/bold]", default=True)
        if not proceed:
            console.print("[yellow]Initialization cancelled[/yellow]")
            return

    # Create template structure
    try:
        create_template_structure(template_info, template_path, skip_examples, console)

        # Show created structure
        console.print("\n[green]✓ Template created successfully![/green]")

        # Display tree structure
        tree = Tree(f"[bold]{template_info['name']}/[/bold]")
        tree.add("[bold]metadata.yaml[/bold] - Template metadata")
        tree.add("[bold]README.md[/bold] - Documentation")

        versions_tree = tree.add(f"[bold]versions/[/bold]")
        version_tree = versions_tree.add(f"[bold]{template_info['version']}/[/bold]")
        version_tree.add("Chart.yaml - Helm chart definition")
        version_tree.add("values.yaml - Configuration values")
        version_tree.add("templates/policy.yaml - Policy template")
        converters_tree = version_tree.add("converters/")
        converters_tree.add("example.yaml - Example converter")

        if not skip_examples:
            examples_tree = tree.add("[bold]examples/[/bold]")
            examples_tree.add("minimal.yaml - Minimal configuration")
            examples_tree.add("standard.yaml - Standard configuration")
            examples_tree.add("production.yaml - Production configuration")

        console.print(tree)

        # Next steps
        console.print("\n[bold]Next Steps:[/bold]")
        console.print(f"  1. Edit metadata.yaml to refine template details")
        console.print(f"  2. Update values.yaml in versions/{template_info['version']}/")
        console.print(f"  3. Add converters in versions/{template_info['version']}/converters/")
        console.print(f"  4. Create examples in examples/ directory")
        console.print(f"  5. Update README.md with documentation")
        console.print(f"  6. Validate: policystack validate {template_path}")
        console.print(f"  7. Test: policystack install {template_info['name']} --dry-run")

    except Exception as e:
        console.print(f"[red]Failed to create template: {e}[/red]")
        if ctx.debug:
            console.print_exception()


def create_template_structure(
    info: Dict,
    template_path: Path,
    skip_examples: bool,
    console: Console,
) -> None:
    """Create the template directory structure and files."""

    # Create directories
    template_path.mkdir(parents=True, exist_ok=True)
    (template_path / "versions" / info["version"]).mkdir(parents=True)
    (template_path / "versions" / info["version"] / "converters").mkdir()
    (template_path / "versions" / info["version"] / "templates").mkdir()

    if not skip_examples:
        (template_path / "examples").mkdir()

    # Create metadata.yaml
    metadata = {
        "name": info["name"],
        "displayName": info["display_name"],
        "description": info["description"],
        "author": {},
        "categories": {
            "primary": info["category"],
            "secondary": [],
        },
        "tags": info["tags"],
        "version": {
            "latest": info["version"],
            "supported": [info["version"]],
            "deprecated": [],
        },
        "versions": {
            info["version"]: {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "policyLibrary": f">={info['policy_library_version']}",
                "openshift": f">={info['openshift_version']}",
                "acm": f">={info['acm_version']}",
                "changes": ["Initial release"],
                "breaking": False,
            }
        },
        "requirements": {
            "required": [
                f"OpenShift Container Platform {info['openshift_version']}+",
                f"Red Hat Advanced Cluster Management {info['acm_version']}+",
                f"PolicyStack library chart {info['policy_library_version']}+",
            ],
            "optional": [],
        },
    }

    # Add author fields only if they have values
    if info["author_name"]:
        metadata["author"]["name"] = info["author_name"]
    if info["author_email"]:
        metadata["author"]["email"] = info["author_email"]
    if info["author_github"]:
        metadata["author"]["github"] = info["author_github"]

    # If no author info provided, use empty dict
    if not metadata["author"]:
        metadata["author"] = {"name": "Unknown"}

    with open(template_path / "metadata.yaml", "w") as f:
        yaml.dump(metadata, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Create README.md
    readme = f"""# {info['display_name']}

{info['description']}

## Overview

This PolicyStack template provides configuration for {info['display_name']}.

## Requirements

### Required
- OpenShift Container Platform {info['openshift_version']}+
- Red Hat Advanced Cluster Management {info['acm_version']}+
- PolicyStack library chart {info['policy_library_version']}+

### Optional
- Additional requirements as needed

## Installation

```bash
# Install using PolicyStack CLI
policystack install {info['name']} --version {info['version']}

# Or manually copy to your stack
cp -r versions/{info['version']}/* /path/to/your/stack/{info['name']}/
```

## Configuration

Key configuration options in `values.yaml`:

```yaml
stack:
  {to_camel_case(info['name'])}:
    enable: true
    # Add configuration details here
```

## Examples

See the `examples/` directory for sample configurations:

- `minimal.yaml` - Basic configuration with minimal settings
- `standard.yaml` - Standard production configuration
- `production.yaml` - Full production configuration with HA

## Changelog

### {info['version']} - {datetime.now().strftime('%Y-%m-%d')}
- Initial release

## Support

For issues and questions, please open an issue in the PolicyStack marketplace repository.

## License

Apache License 2.0
"""

    with open(template_path / "README.md", "w") as f:
        f.write(readme)

    # Create Chart.yaml
    chart_content = f"""apiVersion: v2
name: {info["name"]}
description: Element for {info['display_name']}. {info['description']}
type: application
version: 0.1.0
appVersion: "1.16.0"
dependencies:
  - name: policy-library
    version: "{info['policy_library_version']}"
    repository: "https://policystack.github.io/PolicyStack-chart"
"""

    version_path = template_path / "versions" / info["version"]
    with open(version_path / "Chart.yaml", "w") as f:
        f.write(chart_content)

    # Create values.yaml with camelCase key
    camel_case_name = to_camel_case(info["name"])

    # Generate values.yaml content with proper comments
    values_content = f"""stack:
  # @description: {info['display_name']} element
  {camel_case_name}:
    # @desc: Master control to enable/disable all policies in this element
    enable: false
    
    # @description: Default configuration applied to all policies unless explicitly overridden
    defaultPolicy:
      # @desc: Categories for organizing policies in ACM console and reports
      categories:
        - CM Configuration Management
      # @desc: Specific security controls addressed by these policies
      controls:
        - CM-2 Baseline Configuration
      # @desc: Compliance standards and frameworks
      standards:
        - NIST SP 800-53
      # @desc: Default severity for policy violations (low/medium/high/critical)
      severity: medium
      # @desc: Default action when violations detected (inform/enforce)
      remediationAction: inform
      # @desc: Whether policies start disabled in ACM (useful for testing)
      disabled: false
    
    # @description: Main policy definitions that group related configurations and operators
    policies:
      # @description: Policy for {info['display_name']}
      - name: {info['name']}-policy
        enabled: true
        namespace: open-cluster-management
        description: "Policy for {info['display_name']}"
        remediationAction: inform
        severity: medium
        disabled: false
    
    # @description: Configuration policies for managing Kubernetes resources
    configPolicies:
      # @description: Configuration for {info['display_name']}
      - name: {info['name']}-config
        enabled: true
        description: "Configuration for {info['display_name']}"
        policyRef: {info['name']}-policy
        severity: medium
        remediationAction: inform
        complianceType: musthave
    
    # @description: Operator policies for managing OpenShift/Kubernetes operators
    operatorPolicies: []
"""

    with open(version_path / "values.yaml", "w") as f:
        f.write(values_content)

    # Create templates/policy.yaml
    with open(version_path / "templates" / "policy.yaml", "w") as f:
        f.write('{{- include "policy-library.render" . -}}\n')

    # Create example converter
    converter_example = """# Example converter manifest
# Replace this with actual Kubernetes resources
apiVersion: v1
kind: ConfigMap
metadata:
  name: example-config
  namespace: '{{ .namespace }}'
data:
  key: value
"""

    with open(version_path / "converters" / "example.yaml", "w") as f:
        f.write(converter_example)

    # Create .gitignore
    gitignore = """Chart.lock
*.tgz
charts/
"""

    with open(version_path / ".gitignore", "w") as f:
        f.write(gitignore)

    # Create example files
    if not skip_examples:
        create_example_files(template_path / "examples", info)


def create_example_files(examples_path: Path, info: Dict) -> None:
    """Create example configuration files."""

    camel_case_name = to_camel_case(info["name"])

    # Minimal example
    minimal_content = f"""stack:
  {camel_case_name}:
    enable: true
"""

    with open(examples_path / "minimal.yaml", "w") as f:
        f.write(minimal_content)

    # Standard example
    standard_content = f"""stack:
  {camel_case_name}:
    enable: true
    defaultPolicy:
      remediationAction: enforce
      severity: high
    policies:
      - name: {info['name']}-policy
        enabled: true
        remediationAction: enforce
"""

    with open(examples_path / "standard.yaml", "w") as f:
        f.write(standard_content)

    # Production example
    production_content = f"""stack:
  {camel_case_name}:
    enable: false
    defaultPolicy:
      categories:
        - CM Configuration Management
        - SI System and Information Integrity
      controls:
        - CM-2 Baseline Configuration
        - SI-2 Flaw Remediation
      standards:
        - NIST SP 800-53
        - PCI-DSS
      severity: high
      remediationAction: enforce
      disabled: false
    policies:
      - name: {info['name']}-policy
        enabled: true
        namespace: open-cluster-management
        description: "Production policy for {info['display_name']}"
        remediationAction: enforce
        severity: high
        disabled: false
    configPolicies:
      - name: {info['name']}-config
        enabled: true
        description: "Production configuration for {info['display_name']}"
        policyRef: {info['name']}-policy
        severity: high
        remediationAction: enforce
        complianceType: musthave
"""

    with open(examples_path / "production.yaml", "w") as f:
        f.write(production_content)
