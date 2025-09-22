"""Repository management commands for PolicyStack CLI."""

import asyncio
import json
from typing import Optional

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.table import Table

from ..core.git_repository import GitRepositoryHandler
from ..models import RepositoryConfig


@click.group()
def repo() -> None:
    """
    Manage marketplace repositories.

    Configure multiple sources for PolicyStack templates including
    official, community, and private repositories.
    """


@repo.command()
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output as JSON",
)
@click.pass_obj
def list(ctx, output_json: bool) -> None:
    """
    List configured repositories.

    Shows all configured repositories with their status, priority,
    and template counts.

    Example:
        policystack repo list
    """
    console: Console = ctx.console
    config = ctx.config
    marketplace = ctx.marketplace

    repositories = marketplace.repositories

    if not repositories:
        console.print("[yellow]No repositories configured[/yellow]")
        console.print("\nAdd a repository with: policystack repo add <name> <url>")
        return

    if output_json:

        output = []
        for repo in repositories:
            output.append(
                {
                    "name": repo.name,
                    "url": repo.url,
                    "type": repo.type.value,
                    "enabled": repo.enabled,
                    "priority": repo.priority,
                    "templates": repo.templates_count,
                    "last_updated": repo.last_updated,
                }
            )
        console.print(json.dumps(output, indent=2))
    else:
        # Create table
        table = Table(
            title="Configured Repositories",
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("Name", style="green", no_wrap=True)
        table.add_column("Type", style="blue", no_wrap=True)
        table.add_column("URL/Path", style="white")
        table.add_column("Priority", style="yellow", justify="center")
        table.add_column("Templates", style="cyan", justify="center")
        table.add_column("Status", style="magenta", no_wrap=True)

        for repo in repositories:
            status = "✓ Enabled" if repo.enabled else "✗ Disabled"
            templates = str(repo.templates_count) if repo.templates_count else "-"

            # Truncate URL if too long
            url = repo.display_url
            if len(url) > 40:
                url = url[:37] + "..."

            table.add_row(
                repo.name,
                repo.type.value,
                url,
                str(repo.priority),
                templates,
                status,
            )

        console.print(table)


@repo.command()
@click.argument("name")
@click.argument("url")
@click.option(
    "--type",
    "repo_type",
    type=click.Choice(["git", "local", "http"]),
    default="git",
    help="Repository type",
)
@click.option(
    "--branch",
    help="Git branch (for git repositories)",
)
@click.option(
    "--priority",
    type=int,
    default=50,
    help="Repository priority (0-100, lower = higher priority)",
)
@click.option(
    "--disable",
    is_flag=True,
    help="Add repository in disabled state",
)
@click.option(
    "--auth-token",
    help="Authentication token for private repositories",
    envvar="POLICYSTACK_AUTH_TOKEN",
)
@click.pass_obj
def add(
    ctx,
    name: str,
    url: str,
    repo_type: str,
    branch: Optional[str],
    priority: int,
    disable: bool,
    auth_token: Optional[str],
) -> None:
    """
    Add a new repository.

    Configure a new source for PolicyStack templates. Supports Git,
    local directories, and HTTP endpoints.

    Examples:

    \b
        # Add official repository
        policystack repo add official https://github.com/PolicyStack/marketplace

    \b
        # Add local repository
        policystack repo add local ~/my-templates --type local

    \b
        # Add with custom branch and priority
        policystack repo add testing https://github.com/user/repo --branch dev --priority 10
    """
    console: Console = ctx.console
    config = ctx.config
    marketplace = ctx.marketplace

    # Check if repository already exists
    if marketplace.get_repository(name):
        console.print(f"[yellow]Repository '{name}' already exists[/yellow]")
        console.print("Use 'policystack repo update' to modify it")
        return

    # Validate Git repository if type is git
    if repo_type == "git":

        git_handler = GitRepositoryHandler(marketplace.cache_dir)
        is_valid, validation_message = git_handler.validate_repository(url)

        if not is_valid:
            console.print(f"[red]Invalid Git repository: {validation_message}[/red]")
            console.print("\nPlease check:")
            console.print("  • The URL is correct")
            console.print("  • The repository is accessible")
            console.print("  • Authentication is not required (or provide --auth-token)")
            return

    # Create repository config
    try:
        repo_config = RepositoryConfig(
            name=name,
            url=url,
            type=repo_type,
            branch=branch,
            enabled=not disable,
            priority=priority,
            auth_token=auth_token,
        )
    except Exception as e:
        console.print(f"[red]Invalid repository configuration: {e}[/red]")
        return

    # Add to marketplace
    repo = marketplace.add_repository(repo_config)

    # Add to config
    config.config.add_repository(repo_config)
    config.save()

    console.print(f"[green]✓ Added repository '{name}'[/green]")

    # Try to update the repository
    async def update_repo():
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            progress.add_task(f"Fetching registry from {name}...", total=None)
            success, message = await marketplace.update_repository(repo)
            return success, message

    try:
        success, message = asyncio.run(update_repo())
        if success:
            console.print(f"[green]✓ {message}[/green]")
            console.print(f"  Found {repo.templates_count} templates")
        else:
            console.print(f"[yellow]⚠ Could not fetch registry: {message}[/yellow]")
            console.print("  Repository added but may need manual update")
    except Exception as e:
        console.print(f"[yellow]⚠ Repository added but update failed: {e}[/yellow]")


@repo.command()
@click.argument("name")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation",
)
@click.pass_obj
def remove(ctx, name: str, yes: bool) -> None:
    """
    Remove a repository.

    Remove a configured repository from the marketplace.

    Example:
        policystack repo remove community
    """
    console: Console = ctx.console
    config = ctx.config
    marketplace = ctx.marketplace

    repo = marketplace.get_repository(name)
    if not repo:
        console.print(f"[red]Repository '{name}' not found[/red]")
        return

    if not yes:

        if not Confirm.ask(f"Remove repository '{name}'?", default=False):
            console.print("[yellow]Cancelled[/yellow]")
            return

    # Remove from marketplace
    marketplace.remove_repository(name)

    # Remove from config
    config_repo = config.config.get_repository(name)
    if config_repo:
        config.config.repositories.remove(config_repo)
        config.save()

    console.print(f"[green]✓ Removed repository '{name}'[/green]")


@repo.command()
@click.argument("name", required=False)
@click.option(
    "--all",
    "-a",
    "update_all",
    is_flag=True,
    help="Update all repositories",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force update even if cache is valid",
)
@click.pass_obj
def update(
    ctx,
    name: Optional[str],
    update_all: bool,
    force: bool,
) -> None:
    """
    Update repository registry.

    Fetch the latest template registry from repositories.

    Examples:

    \b
        # Update specific repository
        policystack repo update official

    \b
        # Update all repositories
        policystack repo update --all

    \b
        # Force update ignoring cache
        policystack repo update official --force
    """
    console: Console = ctx.console
    marketplace = ctx.marketplace

    if not name and not update_all:
        console.print("[yellow]Specify a repository name or use --all[/yellow]")
        return

    async def update_repos():
        repos_to_update = []

        if update_all:
            repos_to_update = marketplace.repositories
        elif name:
            repo = marketplace.get_repository(name)
            if not repo:
                console.print(f"[red]Repository '{name}' not found[/red]")
                return
            repos_to_update = [repo]

        if not repos_to_update:
            console.print("[yellow]No repositories to update[/yellow]")
            return

        results = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            for repo in repos_to_update:
                if not repo.enabled:
                    results.append((repo.name, False, "Repository disabled"))
                    continue

                task = progress.add_task(f"Updating {repo.name}...", total=None)

                try:
                    success, message = await marketplace.update_repository(repo, force=force)
                    results.append((repo.name, success, message))
                except Exception as e:
                    results.append((repo.name, False, str(e)))

                progress.remove_task(task)

        # Show results
        console.print("\n[bold]Update Results:[/bold]")
        for repo_name, success, message in results:
            if success:
                console.print(f"  [green]✓[/green] {repo_name}: {message}")
            else:
                console.print(f"  [red]✗[/red] {repo_name}: {message}")

    asyncio.run(update_repos())


@repo.command()
@click.argument("name")
@click.pass_obj
def enable(ctx, name: str) -> None:
    """
    Enable a repository.

    Enable a disabled repository to include it in searches.

    Example:
        policystack repo enable community
    """
    console: Console = ctx.console
    config = ctx.config
    marketplace = ctx.marketplace

    repo = marketplace.get_repository(name)
    if not repo:
        console.print(f"[red]Repository '{name}' not found[/red]")
        return

    if repo.enabled:
        console.print(f"[yellow]Repository '{name}' is already enabled[/yellow]")
        return

    # Enable in marketplace
    repo.enabled = True

    # Update config
    config_repo = config.config.get_repository(name)
    if config_repo:
        config_repo.enabled = True
        config.save()

    console.print(f"[green]✓ Enabled repository '{name}'[/green]")


@repo.command()
@click.argument("name")
@click.pass_obj
def disable(ctx, name: str) -> None:
    """
    Disable a repository.

    Disable a repository to exclude it from searches.

    Example:
        policystack repo disable community
    """
    console: Console = ctx.console
    config = ctx.config
    marketplace = ctx.marketplace

    repo = marketplace.get_repository(name)
    if not repo:
        console.print(f"[red]Repository '{name}' not found[/red]")
        return

    if not repo.enabled:
        console.print(f"[yellow]Repository '{name}' is already disabled[/yellow]")
        return

    # Disable in marketplace
    repo.enabled = False

    # Update config
    config_repo = config.config.get_repository(name)
    if config_repo:
        config_repo.enabled = False
        config.save()

    console.print(f"[green]✓ Disabled repository '{name}'[/green]")


@repo.command()
@click.argument("name")
@click.option("--priority", type=int, help="New priority (0-100)")
@click.option("--url", help="New URL")
@click.option("--branch", help="New branch")
@click.pass_obj
def edit(
    ctx,
    name: str,
    priority: Optional[int],
    url: Optional[str],
    branch: Optional[str],
) -> None:
    """
    Edit repository settings.

    Modify repository configuration such as priority, URL, or branch.

    Examples:

    \b
        # Change priority
        policystack repo edit community --priority 25

    \b
        # Change branch
        policystack repo edit testing --branch main

    \b
        # Change URL
        policystack repo edit local --url ~/new-templates
    """
    console: Console = ctx.console
    config = ctx.config
    marketplace = ctx.marketplace

    repo = marketplace.get_repository(name)
    if not repo:
        console.print(f"[red]Repository '{name}' not found[/red]")
        return

    config_repo = config.config.get_repository(name)
    if not config_repo:
        console.print(f"[red]Repository '{name}' not in configuration[/red]")
        return

    # Track changes
    changes = []

    # Update priority
    if priority is not None:
        if not 0 <= priority <= 100:
            console.print("[red]Priority must be between 0 and 100[/red]")
            return
        repo.priority = priority
        config_repo.priority = priority
        changes.append(f"priority → {priority}")

    # Update URL
    if url:
        repo.url = url
        config_repo.url = url
        changes.append(f"URL → {url}")

    # Update branch
    if branch:
        repo.branch = branch
        config_repo.branch = branch
        changes.append(f"branch → {branch}")

    if not changes:
        console.print("[yellow]No changes specified[/yellow]")
        return

    # Save config
    config.save()

    # Re-sort repositories by priority
    marketplace.repositories.sort(key=lambda r: r.priority)

    console.print(f"[green]✓ Updated repository '{name}'[/green]")
    for change in changes:
        console.print(f"  • {change}")
