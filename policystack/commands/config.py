"""Configuration management commands for PolicyStack CLI."""

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import click
import yaml
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from ..models.config import ConfigModel


@click.group()
def config() -> None:
    """
    Manage PolicyStack CLI configuration.

    View and modify CLI settings including repositories, cache,
    and output preferences.
    """


@config.command()
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output as JSON",
)
@click.pass_obj
def show(ctx, output_json: bool) -> None:
    """
    Show current configuration.

    Display all configuration settings and their current values.

    Example:
        policystack config show
    """
    console: Console = ctx.console
    config = ctx.config.config

    if output_json:

        console.print(json.dumps(config.model_dump(), indent=2, default=str))
    else:
        # Create configuration display
        console.print("[bold cyan]PolicyStack CLI Configuration[/bold cyan]\n")

        # Basic settings
        basic_table = Table(show_header=False, box=None, padding=(0, 2))
        basic_table.add_column("Setting", style="bold")
        basic_table.add_column("Value")

        basic_table.add_row("Config Version", config.version)
        basic_table.add_row("Default Stack Path", str(config.default_stack_path))
        basic_table.add_row("Cache Directory", str(config.cache_dir))
        basic_table.add_row("Default Repository", config.default_repository)
        basic_table.add_row("Auto Update", "Yes" if config.auto_update else "No")
        basic_table.add_row("Update Check Interval", f"{config.update_check_interval}s")
        basic_table.add_row("Output Format", config.output_format)
        basic_table.add_row("Log Level", config.log_level)
        basic_table.add_row("Telemetry", "Enabled" if config.telemetry_enabled else "Disabled")

        console.print(basic_table)

        # Repositories
        if config.repositories:
            console.print("\n[bold]Configured Repositories:[/bold]")
            repo_table = Table(show_header=True, header_style="bold")
            repo_table.add_column("Name", style="green")
            repo_table.add_column("Type", style="blue")
            repo_table.add_column("Priority", justify="center")
            repo_table.add_column("Status", style="yellow")

            for repo in config.repositories:
                status = "Enabled" if repo.enabled else "Disabled"
                repo_table.add_row(repo.name, repo.type, str(repo.priority), status)

            console.print(repo_table)

        # Config file location
        console.print(f"\n[dim]Config file: {ctx.config._config_path}[/dim]")


@config.command()
@click.argument("key")
@click.argument("value")
@click.pass_obj
def set(ctx, key: str, value: str) -> None:
    """
    Set a configuration value.

    Modify a specific configuration setting.

    Examples:

    \b
        # Set default stack path
        policystack config set default_stack_path ~/my-stack

    \b
        # Set log level
        policystack config set log_level DEBUG

    \b
        # Set output format
        policystack config set output_format plain

    \b
        # Enable telemetry
        policystack config set telemetry_enabled true
    """
    console: Console = ctx.console
    config_obj = ctx.config
    config = config_obj.config

    # Map of valid configuration keys
    valid_keys = {
        "default_stack_path": "default_stack_path",
        "cache_dir": "cache_dir",
        "default_repository": "default_repository",
        "auto_update": "auto_update",
        "update_check_interval": "update_check_interval",
        "output_format": "output_format",
        "log_level": "log_level",
        "telemetry_enabled": "telemetry_enabled",
    }

    # Normalize key
    key_lower = key.lower().replace("-", "_")

    if key_lower not in valid_keys:
        console.print(f"[red]Unknown configuration key: {key}[/red]")
        console.print("\nValid keys:")
        for k in valid_keys:
            console.print(f"  • {k}")
        return

    # Get the actual attribute name
    attr_name = valid_keys[key_lower]

    # Convert value to appropriate type
    converted_value: Any = value
    current_value = getattr(config, attr_name)

    try:
        if isinstance(current_value, bool):
            # Convert to boolean
            converted_value = value.lower() in ["true", "yes", "1", "on"]
        elif isinstance(current_value, int):
            # Convert to integer
            converted_value = int(value)
        elif isinstance(current_value, Path):
            # Convert to Path
            converted_value = Path(value).expanduser()
        else:
            # Keep as string
            converted_value = value

        # Set the value
        setattr(config, attr_name, converted_value)

        # Validate the configuration
        config_obj._config = config_obj.config.model_validate(config.model_dump())

        # Save configuration
        config_obj.save()

        console.print(f"[green]✓ Set {key} = {converted_value}[/green]")

    except ValueError as e:
        console.print(f"[red]Invalid value for {key}: {e}[/red]")
    except Exception as e:
        console.print(f"[red]Failed to set configuration: {e}[/red]")


@config.command()
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation",
)
@click.pass_obj
def reset(ctx, yes: bool) -> None:
    """
    Reset configuration to defaults.

    Remove all customizations and restore default settings.

    Example:
        policystack config reset
    """
    console: Console = ctx.console
    config_obj = ctx.config

    if not yes:

        if not Confirm.ask("Reset configuration to defaults?", default=False):
            console.print("[yellow]Cancelled[/yellow]")
            return

    # Create new default configuration
    config_obj._config = config_obj._create_default_config()

    # Save to file
    config_obj.save()

    console.print("[green]✓ Configuration reset to defaults[/green]")


@config.command()
@click.pass_obj
def edit(ctx) -> None:
    """
    Edit configuration file in editor.

    Open the configuration file in your default editor.

    Example:
        policystack config edit
    """
    console: Console = ctx.console
    config_path = ctx.config._config_path

    # Ensure config file exists
    if not config_path.exists():
        ctx.config.save()

    # Determine editor

    editor = os.environ.get("EDITOR", os.environ.get("VISUAL", ""))

    if not editor:
        # Try common editors
        for cmd in ["code", "vim", "nano", "vi"]:
            if shutil.which(cmd):
                editor = cmd
                break

    if not editor:
        console.print("[red]No editor found. Set EDITOR environment variable[/red]")
        return

    console.print(f"Opening {config_path} in {editor}...")

    try:
        # Open editor
        result = subprocess.run([editor, str(config_path)])

        if result.returncode == 0:
            # Reload configuration
            ctx.config.load()
            console.print("[green]✓ Configuration reloaded[/green]")
        else:
            console.print("[yellow]Editor exited with error[/yellow]")

    except Exception as e:
        console.print(f"[red]Failed to open editor: {e}[/red]")


@config.command()
@click.pass_obj
def path(ctx) -> None:
    """
    Show configuration file path.

    Display the full path to the configuration file.

    Example:
        policystack config path
    """
    console: Console = ctx.console
    config_path = ctx.config._config_path

    console.print(str(config_path))


@config.command()
@click.pass_obj
def validate(ctx) -> None:
    """
    Validate configuration file.

    Check if the configuration file is valid and properly formatted.

    Example:
        policystack config validate
    """
    console: Console = ctx.console
    config_path = ctx.config._config_path

    if not config_path.exists():
        console.print("[yellow]Configuration file does not exist[/yellow]")
        console.print(f"Expected at: {config_path}")
        return

    try:
        # Try to load and validate
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Try to create config model
        config_model = ConfigModel(**data)

        console.print("[green]✓ Configuration is valid[/green]")

        # Show any warnings
        warnings = []

        # Check for deprecated settings
        if "repositories" in data:
            for repo in data["repositories"]:
                if repo.get("type") == "svn":
                    warnings.append(f"Repository '{repo.get('name')}' uses deprecated type 'svn'")

        # Check for missing recommended settings
        if not config_model.repositories:
            warnings.append("No repositories configured")

        if warnings:
            console.print("\n[yellow]Warnings:[/yellow]")
            for warning in warnings:
                console.print(f"  ⚠ {warning}")

    except yaml.YAMLError as e:
        console.print(f"[red]Invalid YAML: {e}[/red]")
    except Exception as e:
        console.print(f"[red]Invalid configuration: {e}[/red]")

        # Try to show which field is problematic
        if hasattr(e, "__notes__"):
            for note in e.__notes__:
                console.print(f"  {note}")


@config.command()
@click.pass_obj
def export(ctx) -> None:
    """
    Export configuration as YAML.

    Output the current configuration in YAML format.

    Example:
        policystack config export > config.yaml
    """
    config = ctx.config.config

    # Export as YAML
    yaml_output = yaml.dump(
        config.model_dump(exclude_none=True, mode="json"),
        default_flow_style=False,
        sort_keys=False,
    )

    # Use plain print for export (no formatting)
    print(yaml_output)


@config.command()
@click.argument("file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--merge",
    is_flag=True,
    help="Merge with existing configuration",
)
@click.pass_obj
def import_config(ctx, file: Path, merge: bool) -> None:
    """
    Import configuration from file.

    Load configuration settings from a YAML file.

    Examples:

    \b
        # Replace current configuration
        policystack config import config.yaml

    \b
        # Merge with existing configuration
        policystack config import additional.yaml --merge
    """
    console: Console = ctx.console
    config_obj = ctx.config

    try:
        with open(file, "r") as f:
            imported_data = yaml.safe_load(f)

        if merge:
            # Merge with existing configuration
            current_data = config_obj.config.model_dump()

            # Merge repositories
            if "repositories" in imported_data:
                existing_repos = {r["name"]: r for r in current_data.get("repositories", [])}
                for repo in imported_data["repositories"]:
                    existing_repos[repo["name"]] = repo
                imported_data["repositories"] = list(existing_repos.values())

            # Merge other settings
            for key, value in imported_data.items():
                if key != "repositories":
                    current_data[key] = value

            imported_data = current_data

        # Validate and set configuration
        from ..models.config import ConfigModel

        config_obj._config = ConfigModel(**imported_data)
        config_obj.save()

        console.print(f"[green]✓ Imported configuration from {file}[/green]")

    except yaml.YAMLError as e:
        console.print(f"[red]Invalid YAML in {file}: {e}[/red]")
    except Exception as e:
        console.print(f"[red]Failed to import configuration: {e}[/red]")
