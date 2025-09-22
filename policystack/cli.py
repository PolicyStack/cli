"""PolicyStack CLI main entry point."""

import logging
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.logging import RichHandler

from .__version__ import __version__
from .commands import config_cmd, info_cmd, install_cmd, repo_cmd, search_cmd
from .config import Config
from .core.marketplace import MarketplaceManager
from .utils.console import setup_console

# Initialize console
console = Console()


class CliContext:
    """CLI context object."""

    def __init__(self) -> None:
        """Initialize CLI context."""
        self.config = Config()
        self.marketplace = MarketplaceManager(cache_dir=self.config.config.cache_dir)
        self.console = console
        self.debug = False

    def setup_repositories(self) -> None:
        """Setup repositories from config."""
        for repo_config in self.config.config.repositories:
            if repo_config.enabled:
                self.marketplace.add_repository(repo_config)


@click.group(invoke_without_command=True)
@click.option(
    "--config",
    "-c",
    "config_path",
    type=click.Path(path_type=Path),
    help="Path to configuration file",
    envvar="POLICYSTACK_CONFIG",
)
@click.option(
    "--debug",
    "-d",
    is_flag=True,
    help="Enable debug output",
    envvar="POLICYSTACK_DEBUG",
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["rich", "plain", "json"]),
    default="rich",
    help="Output format",
)
@click.version_option(version=__version__)
@click.pass_context
def cli(
    ctx: click.Context,
    config_path: Optional[Path],
    debug: bool,
    output: str,
) -> None:
    """
    PolicyStack CLI - Discover and install configuration templates.

    Manage PolicyStack templates from the marketplace, search for configurations,
    and install them into your PolicyStack projects.

    Examples:

    \b
        # Search for logging templates
        policystack search logging

    \b
        # Get information about a template
        policystack info openshift-logging

    \b
        # Install a template
        policystack install openshift-logging --version 1.1.0

    \b
        # List configured repositories
        policystack repo list

    \b
        # Add a new repository
        policystack repo add community https://github.com/example/marketplace
    """
    # Setup logging
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    # Initialize context
    cli_ctx = CliContext()
    cli_ctx.debug = debug

    # Override config path if provided
    if config_path:
        cli_ctx.config = Config(config_path)

    # Setup console output format
    setup_console(output)

    # Load configuration
    cli_ctx.config.load()

    # Setup repositories
    cli_ctx.setup_repositories()

    # Store context
    ctx.obj = cli_ctx

    # Show help if no command provided
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# Register commands
cli.add_command(search_cmd.search)
cli.add_command(info_cmd.info)
cli.add_command(install_cmd.install)
cli.add_command(repo_cmd.repo)
cli.add_command(config_cmd.config)


def main() -> None:
    """Main entry point."""
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(130)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if "--debug" in sys.argv or "-d" in sys.argv:
            console.print_exception()
        sys.exit(1)


if __name__ == "__main__":
    main()
