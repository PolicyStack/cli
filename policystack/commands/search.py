"""Search command for PolicyStack CLI."""

import asyncio
import json
from typing import List, Optional

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from ..models import Template


@click.command()
@click.argument("query", required=False)
@click.option(
    "--category",
    "-c",
    help="Filter by category",
)
@click.option(
    "--tag",
    "-t",
    multiple=True,
    help="Filter by tag (can be used multiple times)",
)
@click.option(
    "--repository",
    "-r",
    multiple=True,
    help="Search specific repositories (can be used multiple times)",
)
@click.option(
    "--all",
    "-a",
    "show_all",
    is_flag=True,
    help="Show all templates without filtering",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=20,
    help="Maximum number of results to show",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output results as JSON",
)
@click.pass_obj
def search(
    ctx,
    query: Optional[str],
    category: Optional[str],
    tag: List[str],
    repository: List[str],
    show_all: bool,
    limit: int,
    output_json: bool,
) -> None:
    """
    Search for templates in the marketplace.

    Search by name, description, tags, or category. If no query is provided,
    lists all available templates.

    Examples:

    \b
        # Search for logging-related templates
        policystack search logging

    \b
        # Search in specific category
        policystack search --category observability

    \b
        # Search with multiple tags
        policystack search -t production-ready -t openshift

    \b
        # Search in specific repository
        policystack search logging -r official

    \b
        # Show all templates
        policystack search --all
    """
    console: Console = ctx.console
    marketplace = ctx.marketplace

    # Validate inputs
    if not query and not category and not tag and not show_all:
        console.print(
            "[yellow]Tip: Use --all to show all templates or provide a search query[/yellow]"
        )
        console.print("\nExamples:")
        console.print("  policystack search logging")
        console.print("  policystack search --category observability")
        console.print("  policystack search --all")
        return

    async def perform_search() -> List[Template]:
        """Perform the search operation."""
        # Update repositories first
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("Updating repositories...", total=None)

            # Update enabled repositories
            for repo in marketplace.repositories:
                if repo.enabled:
                    progress.update(task, description=f"Updating {repo.name}...")
                    await marketplace.update_repository(repo)

            progress.update(task, description="Searching...")

            # Perform search
            results = await marketplace.search(
                query=query or "",
                repositories=list(repository) if repository else None,
                category=category,
                tags=list(tag) if tag else None,
            )

            return results

    # Run search
    try:
        results = asyncio.run(perform_search())
    except Exception as e:
        console.print(f"[red]Search failed: {e}[/red]")
        if ctx.debug:
            console.print_exception()
        return

    # Handle no results
    if not results:
        console.print("[yellow]No templates found matching your criteria[/yellow]")

        # Provide suggestions
        if query:
            console.print(f"\nSuggestions:")
            console.print(f"  • Try a broader search term")
            console.print(f"  • Check available categories: policystack search --all")
            console.print(f"  • Update repositories: policystack repo update")
        return

    # Output results
    if output_json:

        # Convert to JSON-serializable format
        json_results = [
            {
                "name": t.name,
                "display_name": t.display_name,
                "description": t.description,
                "latest_version": t.latest_version,
                "repository": t.repository,
                "category": t.primary_category,
                "tags": t.tags,
                "score": t.search_score if query else 0,
            }
            for t in results[:limit]
        ]
        console.print(json.dumps(json_results, indent=2))
    else:
        # Create results table
        table = Table(
            title=f"Search Results ({len(results)} found, showing {min(limit, len(results))})",
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("Name", style="green", no_wrap=True)
        table.add_column("Version", style="blue", no_wrap=True)
        table.add_column("Description", style="white")
        table.add_column("Category", style="yellow", no_wrap=True)
        table.add_column("Repository", style="magenta", no_wrap=True)

        if query:
            table.add_column("Score", style="cyan", justify="right")

        # Add rows
        for template in results[:limit]:
            # Truncate description if too long
            description = template.description
            if len(description) > 60:
                description = description[:57] + "..."

            row = [
                template.name,
                template.latest_version,
                description,
                template.primary_category,
                template.repository,
            ]

            if query:
                row.append(f"{template.search_score:.0f}")

            table.add_row(*row)

        console.print(table)

        # Show additional info
        if len(results) > limit:
            console.print(
                f"\n[dim]Showing {limit} of {len(results)} results. "
                f"Use --limit to show more.[/dim]"
            )

        # Show how to get more info
        if results:
            console.print(
                f"\n[dim]Use 'policystack info <name>' to get detailed information about a template[/dim]"
            )
