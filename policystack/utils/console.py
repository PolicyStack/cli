"""Console utilities for PolicyStack CLI."""

import os
from typing import Optional

from rich.console import Console
from rich.highlighter import RegexHighlighter
from rich.prompt import Confirm, Prompt
from rich.theme import Theme


class PolicyStackHighlighter(RegexHighlighter):
    """Custom highlighter for PolicyStack output."""

    base_style = "policystack."
    highlights = [
        r"(?P<version>\d+\.\d+\.\d+)",
        r"(?P<template>[\w-]+)",
        r"(?P<path>[\w/\-\.]+\.yaml)",
        r"(?P<url>https?://[^\s]+)",
    ]


# Custom theme for PolicyStack
POLICYSTACK_THEME = Theme(
    {
        "policystack.version": "blue",
        "policystack.template": "green",
        "policystack.path": "cyan",
        "policystack.url": "blue underline",
        "info": "cyan",
        "success": "green",
        "warning": "yellow",
        "error": "red bold",
        "highlight": "yellow",
    }
)


# Global console instance
_console: Optional[Console] = None


def get_console() -> Console:
    """Get or create the global console instance."""
    global _console
    if _console is None:
        _console = Console(
            theme=POLICYSTACK_THEME,
            highlighter=PolicyStackHighlighter(),
        )
    return _console


def setup_console(output_format: str = "rich") -> Console:
    """Setup console based on output format."""
    console = get_console()

    if output_format == "plain":
        console._highlight = False
        console._markup = False
    elif output_format == "json":
        # JSON output is handled by commands
        pass

    # Respect NO_COLOR environment variable
    if os.environ.get("NO_COLOR"):
        console._highlight = False
        console._markup = False
        console._color = False

    return console


def print_success(message: str) -> None:
    """Print success message."""
    console = get_console()
    console.print(f"[success]✓ {message}[/success]")


def print_error(message: str) -> None:
    """Print error message."""
    console = get_console()
    console.print(f"[error]✗ {message}[/error]")


def print_warning(message: str) -> None:
    """Print warning message."""
    console = get_console()
    console.print(f"[warning]⚠ {message}[/warning]")


def print_info(message: str) -> None:
    """Print info message."""
    console = get_console()
    console.print(f"[info]ℹ {message}[/info]")


def confirm(message: str, default: bool = False) -> bool:
    """Ask for confirmation."""

    console = get_console()
    return Confirm.ask(message, default=default, console=console)


def prompt(message: str, default: Optional[str] = None) -> str:
    """Ask for input."""

    console = get_console()
    return Prompt.ask(message, default=default, console=console)


def format_size(size: int) -> str:
    """Format size in bytes to human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


def truncate(text: str, length: int = 80, suffix: str = "...") -> str:
    """Truncate text to specified length."""
    if len(text) <= length:
        return text
    return text[: length - len(suffix)] + suffix
