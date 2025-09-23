"""Configuration models for PolicyStack CLI."""

from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class RepositoryConfig(BaseModel):
    """Repository configuration model."""

    name: str = Field(..., description="Repository name")
    url: str = Field(..., description="Repository URL")
    type: str = Field("git", description="Repository type (git, local, http)")
    enabled: bool = Field(True, description="Whether repository is enabled")
    priority: int = Field(50, description="Repository priority (lower = higher priority)")
    branch: Optional[str] = Field(None, description="Git branch to use")
    auth_token: Optional[str] = Field(None, description="Authentication token")
    cache_ttl: int = Field(3600, description="Cache TTL in seconds")

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: int) -> int:
        """Validate priority is between 0 and 100."""
        if not 0 <= v <= 100:
            raise ValueError("Priority must be between 0 and 100")
        return v

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate repository type."""
        valid_types = ["git", "local", "http"]
        if v not in valid_types:
            raise ValueError(f"Type must be one of {valid_types}")
        return v


class ConfigModel(BaseModel):
    """Main configuration model."""

    version: str = Field("1.0.0", description="Config version")
    default_stack_path: Path = Field(Path.cwd() / "stack", description="Default PolicyStack path")
    cache_dir: Path = Field(Path.home() / ".policystack" / "cache", description="Cache directory")
    repositories: List[RepositoryConfig] = Field(
        default_factory=list, description="Configured repositories"
    )
    default_repository: str = Field("official", description="Default repository name")
    auto_update: bool = Field(True, description="Auto-update repository indexes")
    update_check_interval: int = Field(86400, description="Update check interval in seconds")
    output_format: str = Field("rich", description="Output format (rich, json, plain)")
    log_level: str = Field("INFO", description="Logging level")
    telemetry_enabled: bool = Field(False, description="Enable anonymous telemetry")

    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, v: str) -> str:
        """Validate output format."""
        valid_formats = ["rich", "json", "plain"]
        if v not in valid_formats:
            raise ValueError(f"Output format must be one of {valid_formats}")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v = v.upper()
        if v not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v

    def get_repository(self, name: str) -> Optional[RepositoryConfig]:
        """Get repository by name."""
        for repo in self.repositories:
            if repo.name == name:
                return repo
        return None

    def add_repository(self, repo: RepositoryConfig) -> None:
        """Add or update a repository."""
        existing = self.get_repository(repo.name)
        if existing:
            self.repositories.remove(existing)
        self.repositories.append(repo)
        # Sort by priority
        self.repositories.sort(key=lambda x: x.priority)


class Config:
    """Configuration manager singleton."""

    _instance: Optional["Config"] = None
    _config: Optional[ConfigModel] = None
    _config_path: Path

    def __new__(cls) -> "Config":
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_path: Optional[Path] = None) -> None:
        """Initialize configuration."""
        if config_path:
            self._config_path = config_path
        elif not hasattr(self, "_config_path"):
            self._config_path = Path.home() / ".policystack" / "config.yaml"

    @property
    def config(self) -> ConfigModel:
        """Get configuration model."""
        if self._config is None:
            self.load()
        return self._config  # type: ignore

    def load(self) -> None:
        """Load configuration from file."""

        if self._config_path.exists():
            with open(self._config_path, "r") as f:
                data = yaml.safe_load(f)
                self._config = ConfigModel(**data)
        else:
            # Create default configuration
            self._config = self._create_default_config()
            self.save()

    def save(self) -> None:
        """Save configuration to file."""

        self._config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self._config_path, "w") as f:
            yaml.dump(
                self.config.model_dump(exclude_none=True, mode="json"),
                f,
                default_flow_style=False,
                sort_keys=False,
            )

    def _create_default_config(self) -> ConfigModel:
        """Create default configuration."""
        return ConfigModel(
            repositories=[
                RepositoryConfig(
                    name="official",
                    url="https://github.com/PolicyStack/marketplace",
                    type="git",
                    priority=10,
                    branch="main",
                ),
                # Example of additional repositories
                # RepositoryConfig(
                #     name="community",
                #     url="https://github.com/PolicyStack/community-marketplace",
                #     type="git",
                #     priority=20,
                # ),
                # RepositoryConfig(
                #     name="local",
                #     url="~/my-templates",
                #     type="local",
                #     priority=30,
                # ),
            ]
        )
