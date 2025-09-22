# PolicyStack CLI

**Command-line tool for discovering and installing PolicyStack configuration templates**

[Installation](#installation) | [Quick Start](#quick-start) | [Commands](#commands) | [Configuration](#configuration)

---

## 🎯 Overview

PolicyStack CLI (`policystack`) is a command-line tool that allows you to:

- 🔍 **Search** the PolicyStack marketplace for configuration templates
- 📦 **Browse** multiple template repositories (official and custom)
- 📋 **View** detailed information about templates and their versions
- ⚡ **Install** templates directly into your PolicyStack projects
- 🔧 **Manage** multiple marketplace sources
- 🌐 **Universal Git Support** - works with ANY Git repository (GitHub, GitLab, Bitbucket, etc.)

## 📦 Installation

### From Source (Development)

```bash
# Clone the repository
git clone https://github.com/PolicyStack/cli
cd policystack-cli

# Install in development mode
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

### From PyPI

```bash
pip install policystack-cli
```

## 🚀 Quick Start

### 1. Search for Templates

```bash
# Search for logging-related templates
policystack search logging

# Search by category
policystack search --category observability

# Search with tags
policystack search -t production-ready -t openshift

# Show all available templates
policystack search --all
```

### 2. Get Template Information

```bash
# Show detailed information about a template
policystack info openshift-logging

# Show specific version information
policystack info openshift-logging --version 1.0.0

# Show example configurations
policystack info openshift-logging --examples
```

### 3. Install Templates

```bash
# Install latest version to current directory
policystack install openshift-logging

# Install specific version
policystack install openshift-logging --version 1.0.0

# Install with custom element name
policystack install openshift-logging --element-name my-logging

# Install to specific path
policystack install openshift-logging --path ~/my-policystack

# Use production example as base
policystack install openshift-logging --example production

# Dry run to see what would be installed
policystack install openshift-logging --dry-run
```

## 📖 Commands

### `search` - Search the Marketplace

Search for templates across all configured repositories.

```bash
policystack search [QUERY] [OPTIONS]

Options:
  -c, --category TEXT     Filter by category
  -t, --tag TEXT         Filter by tag (multiple allowed)
  -r, --repository TEXT  Search specific repositories
  -a, --all              Show all templates
  -l, --limit INTEGER    Maximum results to show (default: 20)
  --json                 Output as JSON
```

### `info` - Template Information

Display detailed information about a template.

```bash
policystack info TEMPLATE_NAME [OPTIONS]

Options:
  -v, --version TEXT      Show specific version information
  -r, --repository TEXT   Specify repository
  -e, --examples         Show example configurations
  --json                 Output as JSON
```

### `install` - Install Template

Install a template into your PolicyStack project.

```bash
policystack install TEMPLATE_NAME [OPTIONS]

Options:
  -v, --version TEXT      Template version (default: latest)
  -p, --path PATH        PolicyStack project path
  -n, --element-name TEXT Custom element name
  -r, --repository TEXT   Repository to install from
  -e, --example TEXT      Use example config (minimal/production/advanced)
  -f, --force            Force overwrite if exists
  --dry-run              Show what would be installed
  -y, --yes              Skip confirmation prompts
```

### `repo` - Repository Management

Manage marketplace repositories with **universal Git support**.

```bash
# List configured repositories
policystack repo list

# Add ANY Git repository (GitHub, GitLab, Bitbucket, etc.)
policystack repo add NAME URL [OPTIONS]
  --type TEXT            Repository type (git/local/http)
  --branch TEXT          Git branch or tag
  --priority INTEGER     Priority (0-100, lower = higher priority)
  --auth-token TEXT      Authentication token for private repos

# Examples for different Git platforms
policystack repo add github https://github.com/org/templates.git
policystack repo add gitlab https://gitlab.com/org/templates.git
policystack repo add bitbucket https://bitbucket.org/org/templates.git
policystack repo add private https://git.company.com/templates.git --auth-token TOKEN

# Update repository registry
policystack repo update [NAME]

# Remove a repository
policystack repo remove NAME

# Enable/disable repository
policystack repo enable NAME
policystack repo disable NAME
```

### `config` - Configuration Management

Manage CLI configuration.

```bash
# Show current configuration
policystack config show

# Set configuration value
policystack config set KEY VALUE

# Reset to defaults
policystack config reset

# Edit configuration file
policystack config edit
```

## ⚙️ Configuration

Configuration is stored in `~/.policystack/config.yaml`:

```yaml
version: "1.0.0"
default_stack_path: ./stack
cache_dir: ~/.policystack/cache
repositories:
  - name: official
    url: https://github.com/PolicyStack/marketplace
    type: git
    enabled: true
    priority: 10
    branch: main
  - name: community
    url: https://github.com/PolicyStack/community-marketplace
    type: git
    enabled: true
    priority: 20
  - name: local
    url: ~/my-templates
    type: local
    enabled: false
    priority: 30
default_repository: official
auto_update: true
update_check_interval: 86400
output_format: rich
log_level: INFO
```

### Multiple Repository Support

The CLI supports multiple marketplace sources with **universal Git integration**:

1. **Official Repository** - The main PolicyStack marketplace
2. **Community Repositories** - Community-maintained templates
3. **Private Repositories** - Your own private templates (with auth support)
4. **Local Repositories** - Templates on your local filesystem
5. **ANY Git Platform** - GitHub, GitLab, Bitbucket, Gitea, Gogs, etc.

Features:
- Direct Git operations (no API rate limits)
- Branch and tag support
- Authentication for private repositories
- Local caching for performance
- Works with self-hosted Git servers

Repositories are searched in priority order (lower number = higher priority).

## 🔧 Environment Variables

- `POLICYSTACK_CONFIG` - Path to configuration file
- `POLICYSTACK_DEBUG` - Enable debug output
- `POLICYSTACK_CACHE_DIR` - Cache directory location
- `NO_COLOR` - Disable colored output

## 📁 Directory Structure

After installation, templates are organized as:

```
your-policystack/
├── stack/
│   └── openshift-logging/     # Installed element
│       ├── Chart.yaml         # Helm chart definition
│       ├── values.yaml        # Configuration values
│       ├── converters/        # Resource templates
│       │   ├── clusterlogging.yaml
│       │   └── clusterlogforwarder.yaml
│       ├── examples/          # Example configs
│       │   ├── minimal.yaml
│       │   └── production.yaml
│       └── .gitignore
└── values.yaml                # Global values
```

## 🔍 Search Examples

```bash
# Find all logging templates
policystack search logging

# Find production-ready observability templates
policystack search --category observability -t production-ready

# Search in community repository only
policystack search -r community

# Get all templates with elasticsearch tag
policystack search -t elasticsearch

# Show 50 results instead of default 20
policystack search --all --limit 50
```

## 🚀 Installation Examples

```bash
# Basic installation
policystack install openshift-logging

# Production setup with specific version
policystack install openshift-logging \
  --version 1.1.0 \
  --example production \
  --path ~/prod-stack

# Install from community repository
policystack install custom-monitoring \
  --repository community \
  --element-name monitoring

# Dry run to preview changes
policystack install openshift-logging \
  --dry-run \
  --example production
```

## 🐛 Troubleshooting

### Debug Mode

Enable debug output for troubleshooting:

```bash
# Using flag
policystack --debug search logging

# Using environment variable
export POLICYSTACK_DEBUG=1
policystack search logging
```

### Clear Cache

If you're experiencing issues with stale data:

```bash
# Clear all cache
rm -rf ~/.policystack/cache

# Force repository update
policystack repo update --force
```

### Common Issues

1. **"Template not found"** - Update repositories: `policystack repo update`
2. **"Stack directory not found"** - Specify path: `policystack install -p /path/to/stack`
3. **"Connection timeout"** - Check network/proxy settings
4. **"Invalid configuration"** - Reset config: `policystack config reset`

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
# Clone repository
git clone https://github.com/PolicyStack/cli
cd policystack-cli

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode with all dependencies
pip install -e ".[dev]"

# Run tests - Will add soon
pytest

# Run linting
black policystack tests
isort policystack tests
flake8 policystack tests
mypy policystack

# Install pre-commit hooks
pre-commit install
```

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🏆 Credits

- Asa Moore (@asamoorerh)

---

**Need help?** Open an [issue](https://github.com/PolicyStack/cli/issues)
