"""Git repository handler for PolicyStack CLI."""

import hashlib
import json
import logging
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import git
import yaml
from git import Repo

logger = logging.getLogger(__name__)


class GitRepositoryHandler:
    """Handles Git repository operations for PolicyStack."""

    def __init__(self, cache_dir: Path):
        """Initialize Git repository handler."""
        self.cache_dir = cache_dir / "repos"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_repo_cache_path(self, url: str, branch: Optional[str] = None) -> Path:
        """Get cache path for a repository."""

        # Create a unique directory name based on URL and branch
        key = f"{url}:{branch or 'default'}"
        repo_hash = hashlib.sha256(key.encode()).hexdigest()[:12]

        # Extract repo name from URL for readability
        repo_name = url.rstrip("/").split("/")[-1].replace(".git", "")

        return self.cache_dir / f"{repo_name}_{repo_hash}"

    def clone_or_update_repo(
        self,
        url: str,
        branch: Optional[str] = None,
        force_update: bool = False,
        auth_token: Optional[str] = None,
    ) -> Tuple[bool, Repo, str]:
        """
        Clone or update a Git repository.

        Args:
            url: Git repository URL (HTTPS or SSH)
            branch: Branch/tag to checkout (default: default branch)
            force_update: Force update even if cached
            auth_token: Authentication token for private repos (HTTPS only)

        Returns:
            Tuple of (success, Repo object, message)
        """
        repo_path = self.get_repo_cache_path(url, branch)

        # Prepare URL with authentication if needed
        auth_url = url

        # Only add auth token for HTTPS URLs
        if auth_token and url.startswith("https://"):
            # Parse URL and add token
            if "github.com" in url:
                auth_url = url.replace("https://", f"https://{auth_token}@")
            elif "gitlab.com" in url:
                auth_url = url.replace("https://", f"https://oauth2:{auth_token}@")
            elif "bitbucket.org" in url:
                auth_url = url.replace("https://", f"https://x-token-auth:{auth_token}@")
            else:
                # Generic format: https://token@host/path
                auth_url = url.replace("https://", f"https://{auth_token}@")

        # SSH URLs (git@host:path) are used as-is
        # They rely on SSH keys being properly configured

        try:
            if repo_path.exists() and not force_update:
                # Repository exists, try to pull updates
                logger.info(f"Updating existing repository at {repo_path}")
                repo = Repo(repo_path)

                # Fetch latest changes
                origin = repo.remotes.origin
                origin.fetch()

                # Checkout the desired branch
                if branch:
                    try:
                        repo.git.checkout(branch)
                    except git.GitCommandError:
                        # Try as a tag
                        try:
                            repo.git.checkout(f"tags/{branch}")
                        except git.GitCommandError:
                            # Try to create local branch from remote
                            repo.git.checkout("-b", branch, f"origin/{branch}")

                # Pull latest changes if on a branch (not a detached HEAD)
                if not repo.head.is_detached:
                    origin.pull()

                return True, repo, "Repository updated successfully"
            else:
                # Clone the repository
                if repo_path.exists():
                    logger.info(f"Force updating repository, removing {repo_path}")
                    shutil.rmtree(repo_path)

                logger.info(f"Cloning repository {url} to {repo_path}")

                # Clone with depth=1 for efficiency (shallow clone)
                clone_kwargs = {
                    "depth": 1,
                    "single_branch": True,
                }

                if branch:
                    clone_kwargs["branch"] = branch

                repo = Repo.clone_from(auth_url, repo_path, **clone_kwargs)

                return True, repo, "Repository cloned successfully"

        except git.GitCommandError as e:
            error_msg = f"Git error: {str(e)}"
            logger.error(error_msg)

            # Clean up on failure
            if repo_path.exists() and not any(repo_path.iterdir()):
                repo_path.rmdir()

            return False, None, error_msg
        except Exception as e:
            error_msg = f"Failed to clone/update repository: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg

    def read_file_from_repo(
        self,
        repo: Repo,
        file_path: str,
        ref: Optional[str] = None,
    ) -> Optional[bytes]:
        """
        Read a file from the repository.

        Args:
            repo: Git repository object
            file_path: Path to file within repository
            ref: Git ref (branch/tag/commit) to read from

        Returns:
            File contents as bytes or None if not found
        """
        try:
            if ref:
                # Read from specific ref
                try:
                    return repo.odb.stream(repo.tree(ref)[file_path].binsha).read()
                except KeyError:
                    logger.debug(f"File {file_path} not found at ref {ref}")
                    return None
            else:
                # Read from current HEAD
                try:
                    with open(Path(repo.working_dir) / file_path, "rb") as f:
                        return f.read()
                except FileNotFoundError:
                    logger.debug(f"File {file_path} not found in working directory")
                    return None
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return None

    def get_registry_from_repo(
        self,
        url: str,
        branch: Optional[str] = None,
        force_update: bool = False,
        auth_token: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]], str]:
        """
        Get registry data from a Git repository.

        Args:
            url: Repository URL
            branch: Branch to use
            force_update: Force repository update
            auth_token: Authentication token

        Returns:
            Tuple of (success, registry_data, message)
        """
        # Clone or update repository
        success, repo, message = self.clone_or_update_repo(url, branch, force_update, auth_token)

        if not success or not repo:
            return False, None, message

        # Look for registry file
        registry_files = [
            "registry.json",
            "registry.yaml",
            "registry.yml",
        ]

        for registry_file in registry_files:
            content = self.read_file_from_repo(repo, registry_file)
            if content:
                try:
                    if registry_file.endswith(".json"):
                        registry_data = json.loads(content)
                    else:
                        registry_data = yaml.safe_load(content)

                    return True, registry_data, f"Registry loaded from {registry_file}"
                except Exception as e:
                    logger.error(f"Failed to parse {registry_file}: {e}")
                    continue

        return False, None, "No registry file found in repository"

    def get_template_files(
        self,
        url: str,
        template_path: str,
        version: str,
        branch: Optional[str] = None,
        auth_token: Optional[str] = None,
    ) -> Tuple[bool, Optional[Path], str]:
        """
        Get template files from repository.

        Args:
            url: Repository URL
            template_path: Path to template in repository
            version: Template version
            branch: Repository branch
            auth_token: Authentication token

        Returns:
            Tuple of (success, temp_directory_path, message)
        """
        # Clone or update repository
        success, repo, message = self.clone_or_update_repo(
            url, branch, force_update=False, auth_token=auth_token
        )

        if not success or not repo:
            return False, None, message

        # Create temporary directory for template files
        temp_dir = Path(tempfile.mkdtemp(prefix="policystack_template_"))

        try:
            # Copy template version files
            version_path = Path(repo.working_dir) / template_path / "versions" / version

            if not version_path.exists():
                shutil.rmtree(temp_dir)
                return False, None, f"Version {version} not found in repository"

            # Copy version files
            shutil.copytree(version_path, temp_dir, dirs_exist_ok=True)

            # Copy examples if they exist
            examples_path = Path(repo.working_dir) / template_path / "examples"
            if examples_path.exists():
                shutil.copytree(examples_path, temp_dir / "examples", dirs_exist_ok=True)

            return True, temp_dir, "Template files retrieved successfully"

        except Exception as e:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            return False, None, f"Failed to copy template files: {str(e)}"

    def list_branches(self, repo: Repo) -> list:
        """List all branches in the repository."""
        try:
            return [ref.name.replace("origin/", "") for ref in repo.remotes.origin.refs]
        except Exception as e:
            logger.error(f"Failed to list branches: {e}")
            return []

    def list_tags(self, repo: Repo) -> list:
        """List all tags in the repository."""
        try:
            return [tag.name for tag in repo.tags]
        except Exception as e:
            logger.error(f"Failed to list tags: {e}")
            return []

    def get_file_tree(
        self,
        repo: Repo,
        path: str = "",
        ref: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get file tree structure from repository.

        Args:
            repo: Repository object
            path: Path within repository
            ref: Git ref to use

        Returns:
            Dictionary representing file tree
        """
        tree = {}

        try:
            repo_path = Path(repo.working_dir)
            target_path = repo_path / path if path else repo_path

            if not target_path.exists():
                return tree

            for item in target_path.iterdir():
                if item.name.startswith("."):
                    continue  # Skip hidden files

                if item.is_dir():
                    tree[item.name] = {
                        "type": "directory",
                        "children": self.get_file_tree(repo, str(item.relative_to(repo_path))),
                    }
                else:
                    tree[item.name] = {
                        "type": "file",
                        "size": item.stat().st_size,
                    }
        except Exception as e:
            logger.error(f"Failed to get file tree: {e}")

        return tree

    def cleanup_old_repos(self, max_age_days: int = 7):
        """
        Clean up old repository caches.

        Args:
            max_age_days: Maximum age in days before cleanup
        """

        max_age_seconds = max_age_days * 24 * 60 * 60
        current_time = time.time()

        for repo_dir in self.cache_dir.iterdir():
            if repo_dir.is_dir():
                # Check last modification time
                mtime = repo_dir.stat().st_mtime
                if current_time - mtime > max_age_seconds:
                    logger.info(f"Cleaning up old repository cache: {repo_dir}")
                    try:
                        shutil.rmtree(repo_dir)
                    except Exception as e:
                        logger.error(f"Failed to cleanup {repo_dir}: {e}")

    def validate_repository(self, url: str) -> Tuple[bool, str]:
        """
        Validate if URL points to a valid Git repository.

        Args:
            url: Repository URL (HTTPS or SSH)

        Returns:
            Tuple of (is_valid, message)
        """
        try:
            # Use ls-remote to check repository without cloning
            git.cmd.Git().ls_remote(url)
            return True, "Valid Git repository"
        except git.GitCommandError as e:
            error_str = str(e).lower()
            if "not found" in error_str or "does not exist" in error_str:
                return False, "Repository not found"
            elif "authentication" in error_str or "permission" in error_str:
                if url.startswith("git@") or url.startswith("ssh://"):
                    return False, "SSH authentication failed. Check your SSH keys"
                else:
                    return False, "Authentication required. Use --auth-token for private repos"
            elif "could not resolve" in error_str:
                return False, "Could not resolve hostname"
            else:
                return False, f"Invalid repository: {str(e)[:100]}"
        except Exception as e:
            return False, f"Failed to validate repository: {str(e)}"
