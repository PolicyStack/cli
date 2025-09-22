"""Registry parser for PolicyStack CLI."""

import logging
from typing import Any, Dict

from ..models import (
    TemplateAuthor,
    TemplateCategories,
    TemplateComplexity,
    TemplateFeature,
    TemplateMetadata,
    TemplateRequirements,
    TemplateVersion,
    TemplateVersionDetails,
)

logger = logging.getLogger(__name__)


class RegistryParser:
    """Parses registry data into template models."""

    def parse_template(self, data: Dict[str, Any]) -> TemplateMetadata:
        """Parse template data from registry."""
        # Parse author
        author = None
        if "author" in data and data["author"]:
            author = TemplateAuthor(**data["author"])

        # Parse categories
        categories = TemplateCategories(
            primary=data.get("categories", {}).get("primary", "uncategorized"),
            secondary=data.get("categories", {}).get("secondary", []),
        )

        # Parse version info
        version = TemplateVersion(
            latest=data.get("version", {}).get("latest", "0.0.0"),
            supported=data.get("version", {}).get("supported", []),
            deprecated=data.get("version", {}).get("deprecated", []),
        )

        # Parse version details
        versions = {}
        if "versions" in data and data["versions"]:
            for ver, details in data["versions"].items():
                if isinstance(details, dict):
                    versions[ver] = TemplateVersionDetails(
                        date=details.get("date", ""),
                        policy_library=details.get(
                            "policyLibrary", details.get("policy_library", "")
                        ),
                        openshift=details.get("openshift", ""),
                        acm=details.get("acm", ""),
                        operator_version=details.get(
                            "operatorVersion", details.get("operator_version")
                        ),
                        changes=details.get("changes", []),
                        breaking=details.get("breaking", False),
                        migration=details.get("migration"),
                    )

        # Parse features
        features = []
        if "features" in data and data["features"]:
            if isinstance(data["features"], list):
                for feature in data["features"]:
                    if isinstance(feature, dict):
                        features.append(
                            TemplateFeature(
                                name=feature.get("name", ""),
                                description=feature.get("description", ""),
                                icon=feature.get("icon"),
                            )
                        )
            elif isinstance(data["features"], int):
                # Some registries might just have a count
                pass

        # Parse requirements
        requirements = None
        if "requirements" in data and data["requirements"]:
            requirements = TemplateRequirements(
                required=data["requirements"].get("required", []),
                optional=data["requirements"].get("optional", []),
            )

        # Parse complexity
        complexity = None
        if "complexity" in data and data["complexity"]:
            complexity = {}
            for level, info in data["complexity"].items():
                if isinstance(info, dict):
                    complexity[level] = TemplateComplexity(
                        description=info.get("description", ""),
                        estimated_time=info.get("estimatedTime", info.get("estimated_time", "")),
                        skill_level=info.get("skillLevel", info.get("skill_level", "intermediate")),
                    )

        # Create metadata
        metadata = TemplateMetadata(
            name=data.get("name", ""),
            display_name=data.get("displayName", data.get("display_name", data.get("name", ""))),
            description=data.get("description", ""),
            author=author,
            categories=categories,
            tags=data.get("tags", []),
            version=version,
            versions=versions,
            features=features,
            requirements=requirements,
            complexity=complexity,
            support=data.get("support"),
            validation=data.get("validation"),
        )

        return metadata

    def parse_registry(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse full registry data."""
        registry = {
            "version": data.get("version", "1.0.0"),
            "generated": data.get("generated"),
            "templates": [],
            "categories": data.get("categories", {}),
            "tags": data.get("tags", {}),
            "stats": data.get("stats", {}),
        }

        # Parse templates
        if "templates" in data and data["templates"]:
            for template_data in data["templates"]:
                try:
                    # Each template in the registry might be simplified
                    # Expand it if needed
                    registry["templates"].append(template_data)
                except Exception as e:
                    logger.warning(f"Failed to parse template: {e}")

        return registry
