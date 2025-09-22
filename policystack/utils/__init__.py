"""Utility functions for PolicyStack CLI."""

from .console import (
    confirm,
    format_size,
    get_console,
    print_error,
    print_info,
    print_success,
    print_warning,
    prompt,
    setup_console,
    truncate,
)
from .file_utils import (
    atomic_write,
    calculate_checksum,
    cleanup_directory,
    copy_tree,
    download_file,
    extract_archive,
    get_size,
    safe_copy_tree,
    safe_path_join,
)

__all__ = [
    # Console utilities
    "get_console",
    "setup_console",
    "print_success",
    "print_error",
    "print_warning",
    "print_info",
    "confirm",
    "prompt",
    "format_size",
    "truncate",
    # File utilities
    "download_file",
    "extract_archive",
    "calculate_checksum",
    "safe_path_join",
    "copy_tree",
    "safe_copy_tree",
    "atomic_write",
    "get_size",
    "cleanup_directory",
]
