"""Info command for PolicyStack CLI."""

import asyncio
import json
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table


@click.command()
@click.argument("template_name")
@click.option(
    "--version",
    "-v",
    help="Show specific version information",
)
@click.option(
    "--repository",
    "-r",
    help="Specify repository to search in",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output as JSON",
)
@click.option(
    "--examples",
    "-e",
    is_flag=True,
    help="Show example configurations",
)
@click.pass_obj
def info(
    ctx,
    template_name: str,
    version: Optional[str],
    repository: Optional[str],
    output_json: bool,
    examples: bool,
) -> None:
    """
    Show detailed information about a template.

    Display comprehensive information about a template including versions,
    requirements, features, and configuration examples.

    Examples:

    \b
        # Show info for a template
        policystack info openshift-logging

    \b
        # Show specific version info
        policystack info openshift-logging --version 1.0.0

    \b
        # Show info from specific repository
        policystack info openshift-logging --repository community

    \b
        # Show example configurations
        policystack info openshift-logging --examples
    """
    console: Console = ctx.console
    marketplace = ctx.marketplace

    async def get_template_info():
        """Get template information."""
        # Get the template
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
        template = asyncio.run(get_template_info())
        if not template:
            return
    except Exception as e:
        console.print(f"[red]Failed to get template info: {e}[/red]")
        if ctx.debug:
            console.print_exception()
        return

    # Output as JSON if requested
    if output_json:

        output = {
            "name": template.name,
            "display_name": template.display_name,
            "description": template.description,
            "repository": template.repository,
            "latest_version": template.latest_version,
            "author": template.metadata.author.model_dump() if template.metadata.author else None,
            "categories": {
                "primary": template.primary_category,
                "secondary": template.metadata.categories.secondary,
            },
            "tags": template.tags,
            "versions": {
                "latest": template.metadata.version.latest,
                "supported": template.metadata.version.supported,
                "deprecated": template.metadata.version.deprecated,
            },
            "features": (
                [{"name": f.name, "description": f.description} for f in template.metadata.features]
                if template.metadata.features
                else []
            ),
            "requirements": (
                template.metadata.requirements.model_dump()
                if template.metadata.requirements
                else None
            ),
        }

        # Add version details if requested
        if version:
            version_details = template.metadata.get_version_details(version)
            if version_details:
                output["version_details"] = version_details.model_dump()

        console.print(json.dumps(output, indent=2))
        return

    # Display header
    console.print(
        Panel.fit(
            f"[bold cyan]{template.display_name}[/bold cyan]\n"
            f"[dim]{template.description}[/dim]",
            title=f"Template: {template.name}",
            border_style="cyan",
        )
    )

    # Basic information
    info_table = Table(show_header=False, box=None, padding=(0, 2))
    info_table.add_column("Field", style="bold")
    info_table.add_column("Value")

    info_table.add_row("Repository", f"[magenta]{template.repository}[/magenta]")
    info_table.add_row("Latest Version", f"[green]{template.latest_version}[/green]")
    info_table.add_row("Category", f"[yellow]{template.primary_category}[/yellow]")

    if template.tags:
        tags_str = ", ".join([f"[dim]#{tag}[/dim]" for tag in template.tags[:5]])
        info_table.add_row("Tags", tags_str)

    if template.metadata.author:
        author_str = template.metadata.author.name
        if template.metadata.author.github:
            author_str += f" ({template.metadata.author.github})"
        info_table.add_row("Author", author_str)

    console.print(info_table)

    # Version information
    console.print("\n[bold]📦 Versions[/bold]")
    version_table = Table(show_header=True, header_style="bold")
    version_table.add_column("Version", style="blue")
    version_table.add_column("Status", style="green")
    version_table.add_column("Release Date")
    version_table.add_column("Breaking", style="red")

    # Show specific version or all versions
    versions_to_show = [version] if version else template.metadata.version.supported[:5]

    for ver in versions_to_show:
        ver_details = template.metadata.get_version_details(ver)
        if ver_details:
            status = "✓ Latest" if ver == template.latest_version else "Supported"
            if ver in template.metadata.version.deprecated:
                status = "⚠ Deprecated"

            version_table.add_row(
                ver,
                status,
                ver_details.date,
                "Yes" if ver_details.breaking else "No",
            )

    console.print(version_table)

    # Show version details if specific version requested
    if version:
        ver_details = template.metadata.get_version_details(version)
        if ver_details:
            console.print(f"\n[bold]Version {version} Details[/bold]")

            # Requirements
            req_table = Table(show_header=False, box=None, padding=(0, 2))
            req_table.add_column("Requirement", style="bold")
            req_table.add_column("Version")

            req_table.add_row("PolicyStack Library", ver_details.policy_library)
            req_table.add_row("OpenShift", ver_details.openshift)
            req_table.add_row("ACM", ver_details.acm)
            if ver_details.operator_version:
                req_table.add_row("Operator", ver_details.operator_version)

            console.print(req_table)

            # Changes
            if ver_details.changes:
                console.print("\n[bold]Changes:[/bold]")
                for change in ver_details.changes:
                    console.print(f"  • {change}")

            # Migration instructions
            if ver_details.breaking and ver_details.migration:
                console.print("\n[bold red]⚠ Breaking Changes - Migration Required[/bold red]")
                console.print(Panel(ver_details.migration, border_style="red"))

    # Features
    if template.metadata.features:
        console.print("\n[bold]✨ Features[/bold]")
        for feature in template.metadata.features[:5]:
            icon = feature.icon or "•"
            console.print(f"  {icon} [bold]{feature.name}[/bold]: {feature.description}")

    # Requirements
    if template.metadata.requirements:
        console.print("\n[bold]📋 Requirements[/bold]")

        if template.metadata.requirements.required:
            console.print("\n[dim]Required:[/dim]")
            for req in template.metadata.requirements.required:
                console.print(f"  • {req}")

        if template.metadata.requirements.optional:
            console.print("\n[dim]Optional:[/dim]")
            for req in template.metadata.requirements.optional:
                console.print(f"  • {req}")

    # Complexity levels
    if template.metadata.complexity:
        console.print("\n[bold]📊 Complexity Levels[/bold]")
        complexity_table = Table(show_header=True, header_style="bold")
        complexity_table.add_column("Level", style="cyan")
        complexity_table.add_column("Time", style="yellow")
        complexity_table.add_column("Description")

        for level_name, level_info in template.metadata.complexity.items():
            complexity_table.add_row(
                level_name.capitalize(),
                level_info.estimated_time,
                level_info.description,
            )

        console.print(complexity_table)

    # Examples
    if examples:
        console.print("\n[bold]📝 Example Configurations[/bold]")
        console.print(
            "\nExample files available in the template:\n"
            f"  • examples/minimal.yaml - Basic configuration\n"
            f"  • examples/ha-production.yaml - High availability setup\n"
            f"  • examples/with-forwarding.yaml - External forwarding\n"
        )
        console.print(
            f"\n[dim]View examples at: "
            f"https://github.com/PolicyStack/marketplace/tree/main/templates/{template.name}/examples[/dim]"
        )

    # Support information
    if template.metadata.support:
        console.print("\n[bold]🤝 Support[/bold]")
        support_table = Table(show_header=False, box=None, padding=(0, 2))

        for key, value in template.metadata.support.items():
            support_table.add_row(key.capitalize(), f"[blue]{value}[/blue]")

        console.print(support_table)

    # Installation hint
    console.print("\n[dim]To install this template, use:[/dim]")
    console.print(f"  policystack install {template.name} --version {template.latest_version}")
