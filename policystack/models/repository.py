"""Repository models for PolicyStack CLI."""

import hashlib
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class RepositoryType(Enum):
    """Repository type enumeration."""

    GIT = "git"
    LOCAL = "local"
    HTTP = "http"


class Repository(BaseModel):
    """Repository model."""

    name: str = Field(..., description="Repository name")
    url: str = Field(..., description="Repository URL or path")
    type: RepositoryType = Field(RepositoryType.GIT, description="Repository type")
    branch: Optional[str] = Field(None, description="Git branch")
    enabled: bool = Field(True, description="Repository enabled status")
    priority: int = Field(50, description="Repository priority")
    auth_token: Optional[str] = Field(None, description="Authentication token")
    registry: Optional[Dict[str, Any]] = Field(None, description="Cached registry data")
    last_updated: Optional[str] = Field(None, description="Last update timestamp")
    templates_count: int = Field(0, description="Number of templates")

    @property
    def is_git(self) -> bool:
        """Check if repository is git-based."""
        return self.type == RepositoryType.GIT

    @property
    def is_local(self) -> bool:
        """Check if repository is local."""
        return self.type == RepositoryType.LOCAL

    @property
    def is_http(self) -> bool:
        """Check if repository is HTTP-based."""
        return self.type == RepositoryType.HTTP

    @property
    def display_url(self) -> str:
        """Get display-friendly URL."""
        if self.is_local:
            return str(Path(self.url).expanduser())
        return self.url

    @property
    def cache_key(self) -> str:
        """Get cache key for this repository."""

        key_parts = [self.name, self.url]
        if self.branch:
            key_parts.append(self.branch)

        key_str = "|".join(key_parts)
        return hashlib.sha256(key_str.encode()).hexdigest()[:12]

    def get_templates(self) -> List[Dict[str, Any]]:
        """Get templates from registry."""
        if self.registry and "templates" in self.registry:
            return self.registry["templates"]
        return []

    def get_categories(self) -> Dict[str, List[str]]:
        """Get categories from registry."""
        if self.registry and "categories" in self.registry:
            return self.registry["categories"]
        return {}

    def get_tags(self) -> Dict[str, List[str]]:
        """Get tags from registry."""
        if self.registry and "tags" in self.registry:
            return self.registry["tags"]
        return {}

    def __str__(self) -> str:
        """String representation."""
        return f"{self.name} ({self.type.value})"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"<Repository {self.name}: {self.display_url}>"
