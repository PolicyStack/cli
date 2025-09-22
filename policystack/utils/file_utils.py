"""File utility functions for PolicyStack CLI."""

import hashlib
import logging
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, Union

import httpx

logger = logging.getLogger(__name__)


async def download_file(
    url: str,
    destination: Path,
    chunk_size: int = 8192,
    timeout: int = 30,
) -> Path:
    """
    Download a file from URL to destination.

    Args:
        url: URL to download from
        destination: Path to save file
        chunk_size: Size of chunks to download
        timeout: Request timeout in seconds

    Returns:
        Path to downloaded file
    """
    destination.parent.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient() as client:
        async with client.stream("GET", url, timeout=timeout) as response:
            response.raise_for_status()

            with open(destination, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size):
                    f.write(chunk)

    return destination


def extract_archive(
    archive_path: Path,
    destination: Path,
    strip_components: int = 0,
) -> Path:
    """
    Extract archive (zip, tar, tar.gz) to destination.

    Args:
        archive_path: Path to archive file
        destination: Path to extract to
        strip_components: Number of path components to strip

    Returns:
        Path to extracted content
    """
    destination.mkdir(parents=True, exist_ok=True)

    # Determine archive type
    if archive_path.suffix == ".zip":
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            if strip_components > 0:
                _extract_with_strip(zip_ref, destination, strip_components)
            else:
                zip_ref.extractall(destination)
    elif archive_path.suffix in [".tar", ".gz", ".tgz", ".bz2", ".xz"]:
        mode = "r"
        if archive_path.suffix in [".gz", ".tgz"]:
            mode = "r:gz"
        elif archive_path.suffix == ".bz2":
            mode = "r:bz2"
        elif archive_path.suffix == ".xz":
            mode = "r:xz"

        with tarfile.open(archive_path, mode) as tar_ref:
            if strip_components > 0:
                _extract_tar_with_strip(tar_ref, destination, strip_components)
            else:
                tar_ref.extractall(destination)
    else:
        raise ValueError(f"Unsupported archive type: {archive_path.suffix}")

    return destination


def _extract_with_strip(
    zip_ref: zipfile.ZipFile,
    destination: Path,
    strip_components: int,
) -> None:
    """Extract zip with stripping path components."""
    for member in zip_ref.namelist():
        parts = Path(member).parts
        if len(parts) > strip_components:
            new_path = Path(*parts[strip_components:])
            target = destination / new_path

            if member.endswith("/"):
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with zip_ref.open(member) as source, open(target, "wb") as dest:
                    shutil.copyfileobj(source, dest)


def _extract_tar_with_strip(
    tar_ref: tarfile.TarFile,
    destination: Path,
    strip_components: int,
) -> None:
    """Extract tar with stripping path components."""
    for member in tar_ref.getmembers():
        parts = Path(member.name).parts
        if len(parts) > strip_components:
            member.name = str(Path(*parts[strip_components:]))
            tar_ref.extract(member, destination)


def calculate_checksum(
    file_path: Path,
    algorithm: str = "sha256",
) -> str:
    """
    Calculate checksum of a file.

    Args:
        file_path: Path to file
        algorithm: Hash algorithm (md5, sha1, sha256, sha512)

    Returns:
        Hex digest of checksum
    """
    hash_func = getattr(hashlib, algorithm)()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)

    return hash_func.hexdigest()


def safe_path_join(base: Path, *paths: Union[str, Path]) -> Path:
    """
    Safely join paths preventing directory traversal.

    Args:
        base: Base path
        paths: Paths to join

    Returns:
        Joined path

    Raises:
        ValueError: If resulting path is outside base
    """
    base = base.resolve()
    result = base

    for path in paths:
        result = (result / path).resolve()

        # Check if result is under base
        try:
            result.relative_to(base)
        except ValueError:
            raise ValueError(f"Path traversal detected: {path}")

    return result


def copy_tree(
    src: Path,
    dst: Path,
    ignore: Optional[list] = None,
    symlinks: bool = False,
) -> None:
    """
    Copy directory tree from source to destination.

    Args:
        src: Source directory
        dst: Destination directory
        ignore: List of patterns to ignore
        symlinks: Whether to copy symlinks as symlinks
    """
    ignore_patterns = shutil.ignore_patterns(*ignore) if ignore else None

    shutil.copytree(
        src,
        dst,
        ignore=ignore_patterns,
        symlinks=symlinks,
        dirs_exist_ok=True,
    )


def safe_copy_tree(
    src: Path,
    dst: Path,
    ignore: Optional[list] = None,
) -> None:
    """
    Safely copy directory tree preventing directory traversal.

    Args:
        src: Source directory
        dst: Destination directory
        ignore: List of patterns to ignore
    """
    src = src.resolve()
    dst = dst.resolve()

    # Ensure destination is created safely
    dst.mkdir(parents=True, exist_ok=True)

    copy_tree(src, dst, ignore=ignore, symlinks=False)


def atomic_write(
    file_path: Path,
    content: Union[str, bytes],
    mode: str = "w",
) -> None:
    """
    Atomically write content to file.

    Args:
        file_path: Path to file
        content: Content to write
        mode: File mode (w for text, wb for binary)
    """
    # Create temp file in same directory
    temp_fd, temp_path = tempfile.mkstemp(
        dir=file_path.parent,
        prefix=f".{file_path.name}.",
        suffix=".tmp",
    )

    try:
        with open(temp_fd, mode) as f:
            f.write(content)

        # Atomic rename
        Path(temp_path).replace(file_path)
    except Exception:
        # Clean up temp file on error
        Path(temp_path).unlink(missing_ok=True)
        raise


def get_size(path: Path) -> int:
    """
    Get size of file or directory.

    Args:
        path: Path to file or directory

    Returns:
        Size in bytes
    """
    if path.is_file():
        return path.stat().st_size
    elif path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    else:
        return 0


def cleanup_directory(
    directory: Path,
    keep_files: Optional[list] = None,
) -> None:
    """
    Clean up directory contents.

    Args:
        directory: Directory to clean
        keep_files: List of files/patterns to keep
    """
    if not directory.exists():
        return

    keep_files = keep_files or []

    for item in directory.iterdir():
        # Check if item should be kept
        should_keep = False
        for pattern in keep_files:
            if item.match(pattern):
                should_keep = True
                break

        if not should_keep:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
