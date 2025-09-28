"""Template models for PolicyStack CLI."""

from typing import Any, Dict, List, Optional, Tuple

from packaging import version as pkg_version
from pydantic import BaseModel, Field, field_validator


class TemplateAuthor(BaseModel):
    """Template author information."""

    name: str = Field(..., description="Author name")
    github: Optional[str] = Field(None, description="GitHub handle")
    email: Optional[str] = Field(None, description="Email address")


class TemplateCategories(BaseModel):
    """Template categories."""

    primary: str = Field(..., description="Primary category")
    secondary: List[str] = Field(default_factory=list, description="Secondary categories")


class TemplateComplexity(BaseModel):
    """Template complexity information."""

    description: str = Field(..., description="Complexity description")
    estimated_time: str = Field(..., description="Estimated time to deploy")
    skill_level: str = Field(..., description="Required skill level")

    @field_validator("skill_level")
    @classmethod
    def validate_skill_level(cls, v: str) -> str:
        """Validate skill level."""
        valid_levels = ["beginner", "intermediate", "expert"]
        if v.lower() not in valid_levels:
            raise ValueError(f"Skill level must be one of {valid_levels}")
        return v.lower()

class UpgradeConstraints(BaseModel):
    """Upgrade path constraints for a version."""

    replaces: Optional[str] = Field(None, description="Version this directly replaces")
    skips: List[str] = Field(default_factory=list, description="Versions that can be skipped")
    blocks: List[str] = Field(default_factory=list, description="Versions that block upgrade to this")
    requires_migration: bool = Field(False, description="Whether migration is required")
    pre_upgrade_hook: Optional[str] = Field(None, description="Script to run before upgrade")
    post_upgrade_hook: Optional[str] = Field(None, description="Script to run after upgrade")
    minimum_policystack_cli: Optional[str] = Field(None, description="Minimum CLI version required")

class TemplateVersionDetails(BaseModel):
    """Detailed version information."""

    date: str = Field(..., description="Release date")
    policy_library: str = Field(..., description="PolicyStack library version requirement")
    openshift: str = Field(..., description="OpenShift version requirement")
    acm: str = Field(..., description="ACM version requirement")
    operator_version: Optional[str] = Field(None, description="Operator version")
    changes: List[str] = Field(default_factory=list, description="Changes in this version")
    breaking: bool = Field(False, description="Whether this version has breaking changes")
    migration: Optional[str] = Field(None, description="Migration instructions")
    upgrade: Optional[UpgradeConstraints] = Field(None, description="Upgrade constraints")
    deprecated_features: List[str] = Field(default_factory=list, description="Features deprecated in this version")
    new_features: List[str] = Field(default_factory=list, description="New features in this version")
    
    def can_upgrade_from(self, from_version: str) -> Tuple[bool, str]:
        """Check if upgrade from a version is allowed."""
        if not self.upgrade:
            return True, "No upgrade constraints defined"
        
        # Check if it's a direct replacement
        if self.upgrade.replaces == from_version:
            return True, "Direct upgrade path"
        
        # Check if version can be skipped
        if from_version in self.upgrade.skips:
            return True, "Version can be skipped"
        
        # Check if version blocks upgrade
        if from_version in self.upgrade.blocks:
            return False, f"Cannot upgrade directly from {from_version}. Please upgrade to an intermediate version first."
        
        # Check if we can reach this version through skips
        if self.upgrade.replaces:
            return False, f"Must upgrade from {self.upgrade.replaces} or versions in skip list"
        
        return True, "Upgrade allowed"

class TemplateVersion(BaseModel):
    """Template version information."""

    latest: str = Field(..., description="Latest version")
    supported: List[str] = Field(default_factory=list, description="Supported versions")
    deprecated: List[str] = Field(default_factory=list, description="Deprecated versions")

    @field_validator("latest")
    @classmethod
    def validate_version_format(cls, v: str) -> str:
        """Validate semantic version format."""
        try:
            pkg_version.parse(v)
        except pkg_version.InvalidVersion:
            raise ValueError(f"Invalid version format: {v}")
        return v

    def is_supported(self, version: str) -> bool:
        """Check if version is supported."""
        return version in self.supported

    def is_deprecated(self, version: str) -> bool:
        """Check if version is deprecated."""
        return version in self.deprecated

    def is_latest(self, version: str) -> bool:
        """Check if version is latest."""
        return version == self.latest


class TemplateFeature(BaseModel):
    """Template feature description."""

    name: str = Field(..., description="Feature name")
    description: str = Field(..., description="Feature description")
    icon: Optional[str] = Field(None, description="Feature icon/emoji")


class TemplateRequirements(BaseModel):
    """Template requirements."""

    required: List[str] = Field(default_factory=list, description="Required components")
    optional: List[str] = Field(default_factory=list, description="Optional components")


class TemplateMetadata(BaseModel):
    """Complete template metadata."""

    name: str = Field(..., description="Template name")
    display_name: str = Field(..., description="Display name")
    description: str = Field(..., description="Template description")
    author: TemplateAuthor = Field(..., description="Author information")
    categories: TemplateCategories = Field(..., description="Categories")
    tags: List[str] = Field(default_factory=list, description="Searchable tags")
    version: TemplateVersion = Field(..., description="Version information")
    versions: Dict[str, TemplateVersionDetails] = Field(
        default_factory=dict, description="Version details"
    )
    features: List[TemplateFeature] = Field(default_factory=list, description="Template features")
    requirements: Optional[TemplateRequirements] = Field(None, description="Template requirements")
    complexity: Optional[Dict[str, TemplateComplexity]] = Field(
        None, description="Complexity levels"
    )
    support: Optional[Dict[str, str]] = Field(None, description="Support information")
    validation: Optional[Dict[str, Any]] = Field(None, description="Validation information")

    @property
    def latest_version_details(self) -> Optional[TemplateVersionDetails]:
        """Get details for latest version."""
        return self.versions.get(self.version.latest)

    def get_version_details(self, version: str) -> Optional[TemplateVersionDetails]:
        """Get details for specific version."""
        return self.versions.get(version)


class Template:
    """Template wrapper with repository context."""

    def __init__(
        self,
        metadata: TemplateMetadata,
        repository: str,
        path: str,
    ) -> None:
        """Initialize template."""
        self.metadata = metadata
        self.repository = repository
        self.path = path
        self._score: float = 0.0

    @property
    def name(self) -> str:
        """Get template name."""
        return self.metadata.name

    @property
    def display_name(self) -> str:
        """Get template display name."""
        return self.metadata.display_name

    @property
    def description(self) -> str:
        """Get template description."""
        return self.metadata.description

    @property
    def latest_version(self) -> str:
        """Get latest version."""
        return self.metadata.version.latest

    @property
    def tags(self) -> List[str]:
        """Get template tags."""
        return self.metadata.tags

    @property
    def primary_category(self) -> str:
        """Get primary category."""
        return self.metadata.categories.primary

    @property
    def search_score(self) -> float:
        """Get search score."""
        return self._score

    @search_score.setter
    def search_score(self, value: float) -> None:
        """Set search score."""
        self._score = value

    def matches_query(self, query: str) -> bool:
        """Check if template matches search query."""
        query_lower = query.lower()
        searchable_text = [
            self.name,
            self.display_name,
            self.description,
            self.primary_category,
        ]
        searchable_text.extend(self.tags)
        searchable_text.extend(self.metadata.categories.secondary)

        return any(query_lower in text.lower() for text in searchable_text if text)

    def calculate_relevance(self, query: str) -> float:
        """Calculate relevance score for search query."""
        query_lower = query.lower()
        score = 0.0

        # Exact name match = highest score
        if query_lower == self.name.lower():
            score += 100.0
        elif query_lower in self.name.lower():
            score += 50.0

        # Display name match
        if query_lower in self.display_name.lower():
            score += 30.0

        # Primary category match
        if query_lower == self.primary_category.lower():
            score += 25.0

        # Tag matches
        for tag in self.tags:
            if query_lower == tag.lower():
                score += 20.0
            elif query_lower in tag.lower():
                score += 10.0

        # Description match
        if query_lower in self.description.lower():
            score += 5.0

        # Boost for latest/maintained templates
        if self.metadata.version.supported:
            score += 5.0

        self._score = score
        return score

    def __repr__(self) -> str:
        """String representation."""
        return f"<Template {self.name}@{self.latest_version} from {self.repository}>"
