"""PolicyStack CLI - Discover and install configuration templates."""

from .__version__ import __version__

__author__ = "PolicyStack Team"
__email__ = "team@policystack.io"
__license__ = "Apache-2.0"

# Public API
from .cli import cli, main
from .config import Config
from .core.marketplace import MarketplaceManager
from .models import (
    Repository,
    Template,
    TemplateMetadata,
)

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "__license__",
    "cli",
    "main",
    "Config",
    "MarketplaceManager",
    "Repository",
    "Template",
    "TemplateMetadata",
]
