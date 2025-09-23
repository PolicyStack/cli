"""Marketplace operations for PolicyStack CLI."""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import yaml

from ..models import Repository, RepositoryConfig, RepositoryType, Template
from .git_repository import GitRepositoryHandler
from .registry import RegistryParser

logger = logging.getLogger(__name__)


class MarketplaceManager:
    """Manages marketplace operations across multiple repositories."""

    def __init__(self, cache_dir: Optional[Path] = None) -> None:
        """Initialize marketplace manager."""
        self.cache_dir = cache_dir or Path.home() / ".policystack" / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.repositories: List[Repository] = []
        self.templates_cache: Dict[str, Template] = {}
        self.registry_parser = RegistryParser()
        self.git_handler = GitRepositoryHandler(self.cache_dir)

    def add_repository(self, config: RepositoryConfig) -> Repository:
        """Add a repository to the marketplace."""
        repo = Repository(
            name=config.name,
            url=config.url,
            type=RepositoryType(config.type),
            branch=config.branch,
            enabled=config.enabled,
            priority=config.priority,
            auth_token=config.auth_token,
        )

        # Check if repository already exists
        existing_index = next(
            (i for i, r in enumerate(self.repositories) if r.name == repo.name), None
        )

        if existing_index is not None:
            self.repositories[existing_index] = repo
        else:
            self.repositories.append(repo)

        # Sort by priority
        self.repositories.sort(key=lambda r: r.priority)

        return repo

    def get_repository(self, name: str) -> Optional[Repository]:
        """Get repository by name."""
        return next((r for r in self.repositories if r.name == name), None)

    def remove_repository(self, name: str) -> bool:
        """Remove a repository."""
        repo = self.get_repository(name)
        if repo:
            self.repositories.remove(repo)
            # Clear cache for this repository
            cache_file = self._get_cache_path(repo)
            if cache_file.exists():
                cache_file.unlink()

            # Also cleanup Git repository cache
            self.git_handler.cleanup_old_repos()

            return True
        return False

    async def update_repository(
        self, repository: Repository, force: bool = False
    ) -> Tuple[bool, str]:
        """Update repository registry."""
        if not repository.enabled:
            return False, f"Repository {repository.name} is disabled"

        cache_path = self._get_cache_path(repository)

        # Check cache validity
        if not force and self._is_cache_valid(cache_path, repository):
            logger.info(f"Using cached registry for {repository.name}")
            return self._load_cached_registry(repository, cache_path)

        logger.info(f"Updating registry for {repository.name}")

        try:
            if repository.is_git:
                return await self._update_git_repository(repository)
            elif repository.is_local:
                return await self._update_local_repository(repository)
            elif repository.is_http:
                return await self._update_http_repository(repository)
            else:
                return False, f"Unsupported repository type: {repository.type}"
        except Exception as e:
            logger.error(f"Failed to update {repository.name}: {e}")
            return False, str(e)

    async def _update_git_repository(self, repository: Repository) -> Tuple[bool, str]:
        """Update git-based repository using Git."""
        try:
            # Get authentication token from repository config
            auth_token = getattr(repository, "auth_token", None)

            # Use GitRepositoryHandler to get registry
            success, registry_data, message = self.git_handler.get_registry_from_repo(
                url=repository.url,
                branch=repository.branch,
                force_update=True,
                auth_token=auth_token,
            )

            if success and registry_data:
                return self._process_registry(repository, registry_data)
            else:
                return False, message or "Failed to get registry from Git repository"

        except Exception as e:
            logger.error(f"Failed to update Git repository {repository.name}: {e}")
            return False, str(e)

    async def _update_local_repository(self, repository: Repository) -> Tuple[bool, str]:
        """Update local repository."""
        repo_path = Path(repository.url).expanduser()

        if not repo_path.exists():
            return False, f"Local repository path does not exist: {repo_path}"

        registry_files = [
            repo_path / "registry.json",
            repo_path / "registry.yaml",
        ]

        for registry_file in registry_files:
            if registry_file.exists():
                try:
                    with open(registry_file, "r") as f:
                        if registry_file.suffix == ".json":
                            registry_data = json.load(f)
                        else:
                            registry_data = yaml.safe_load(f)

                    return self._process_registry(repository, registry_data)
                except Exception as e:
                    logger.error(f"Failed to load {registry_file}: {e}")

        return False, f"No registry file found in {repo_path}"

    async def _update_http_repository(self, repository: Repository) -> Tuple[bool, str]:
        """Update HTTP-based repository."""
        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                response = await client.get(repository.url)
                if response.status_code == 200:
                    content = response.text

                    # Try parsing as JSON first, then YAML
                    try:
                        registry_data = json.loads(content)
                    except json.JSONDecodeError:
                        registry_data = yaml.safe_load(content)

                    return self._process_registry(repository, registry_data)
            except Exception as e:
                return False, f"Failed to fetch registry: {e}"

        return False, "Failed to fetch registry"

    def _process_registry(
        self, repository: Repository, registry_data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Process registry data and update repository."""
        try:
            # Validate registry structure
            if "templates" not in registry_data:
                return False, "Invalid registry: missing templates"

            # Update repository with registry data
            repository.registry = registry_data
            repository.last_updated = datetime.now().isoformat()
            repository.templates_count = len(registry_data.get("templates", []))

            # Cache the registry
            cache_path = self._get_cache_path(repository)
            self._save_cache(cache_path, registry_data)

            # Parse templates
            self._parse_templates(repository)

            return True, f"Successfully updated {repository.name}"
        except Exception as e:
            return False, f"Failed to process registry: {e}"

    def _parse_templates(self, repository: Repository) -> None:
        """Parse templates from repository registry."""
        if not repository.registry or "templates" not in repository.registry:
            return

        for template_data in repository.registry["templates"]:
            try:
                # Parse metadata
                metadata = self.registry_parser.parse_template(template_data)

                # Create template instance
                template = Template(
                    metadata=metadata,
                    repository=repository.name,
                    path=template_data.get("path", f"templates/{metadata.name}"),
                )

                # Cache template
                cache_key = f"{repository.name}:{template.name}"
                self.templates_cache[cache_key] = template
            except Exception as e:
                logger.warning(f"Failed to parse template {template_data.get('name')}: {e}")

    async def search(
        self,
        query: str,
        repositories: Optional[List[str]] = None,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> List[Template]:
        """Search for templates across repositories."""
        results: List[Template] = []

        # Filter repositories
        search_repos = self.repositories
        if repositories:
            search_repos = [r for r in self.repositories if r.name in repositories]

        # Search in each repository
        for repo in search_repos:
            if not repo.enabled:
                continue

            # Ensure repository is updated
            if not repo.registry:
                await self.update_repository(repo)

            # Search templates in this repository
            repo_templates = self._get_repository_templates(repo)

            for template in repo_templates:
                # Check query match
                if query and not template.matches_query(query):
                    continue

                # Check category filter
                if category:
                    if template.primary_category != category:
                        if category not in template.metadata.categories.secondary:
                            continue

                # Check tag filters
                if tags:
                    template_tags = set(template.tags)
                    if not template_tags.intersection(tags):
                        continue

                # Calculate relevance if query provided
                if query:
                    template.calculate_relevance(query)

                results.append(template)

        # Sort by relevance score if query was provided
        if query:
            results.sort(key=lambda t: t.search_score, reverse=True)
        else:
            # Sort by name if no query
            results.sort(key=lambda t: t.name)

        return results

    async def get_template(self, name: str, repository: Optional[str] = None) -> Optional[Template]:
        """Get a specific template."""
        # If repository specified, check only that one
        if repository:
            cache_key = f"{repository}:{name}"
            if cache_key in self.templates_cache:
                return self.templates_cache[cache_key]

            # Try updating repository if not in cache
            repo = self.get_repository(repository)
            if repo:
                await self.update_repository(repo)
                if cache_key in self.templates_cache:
                    return self.templates_cache[cache_key]
        else:
            # Search all repositories in priority order
            for repo in self.repositories:
                cache_key = f"{repo.name}:{name}"
                if cache_key in self.templates_cache:
                    return self.templates_cache[cache_key]

            # Try updating all repositories
            for repo in self.repositories:
                if repo.enabled:
                    await self.update_repository(repo)
                    cache_key = f"{repo.name}:{name}"
                    if cache_key in self.templates_cache:
                        return self.templates_cache[cache_key]

        return None

    def _get_repository_templates(self, repository: Repository) -> List[Template]:
        """Get all templates from a repository."""
        templates = []
        for key, template in self.templates_cache.items():
            if key.startswith(f"{repository.name}:"):
                templates.append(template)
        return templates

    def _get_cache_path(self, repository: Repository) -> Path:
        """Get cache file path for repository."""
        return self.cache_dir / f"registry_{repository.cache_key}.json"

    def _is_cache_valid(self, cache_path: Path, repository: Repository) -> bool:
        """Check if cache is still valid."""
        if not cache_path.exists():
            return False

        # Check age (default 1 hour)
        max_age = timedelta(seconds=3600)
        file_age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)

        return file_age < max_age

    def _load_cached_registry(self, repository: Repository, cache_path: Path) -> Tuple[bool, str]:
        """Load registry from cache."""
        try:
            with open(cache_path, "r") as f:
                registry_data = json.load(f)
            return self._process_registry(repository, registry_data)
        except Exception as e:
            logger.error(f"Failed to load cache for {repository.name}: {e}")
            return False, str(e)

    def _save_cache(self, cache_path: Path, data: Dict[str, Any]) -> None:
        """Save registry data to cache."""
        try:
            with open(cache_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
