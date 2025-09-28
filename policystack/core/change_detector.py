"""Change detection system for PolicyStack templates."""

import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml
from deepdiff import DeepDiff


class ChangeType:
    """Types of changes detected."""
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    UNCHANGED = "unchanged"


class FileChange:
    """Represents a change in a file."""
    
    def __init__(self, path: Path, change_type: str, details: Optional[Dict] = None):
        self.path = path
        self.change_type = change_type
        self.details = details or {}
        self.original_hash: Optional[str] = None
        self.current_hash: Optional[str] = None
        
    def is_helm_template(self) -> bool:
        """Check if file is a Helm template."""
        if self.path.suffix in ['.yaml', '.yml', '.tpl']:
            try:
                with open(self.path, 'r') as f:
                    content = f.read()
                    return '{{' in content or '{{-' in content
            except:
                return False
        return False


class TemplateSnapshot:
    """Snapshot of template state for comparison."""
    
    def __init__(self, path: Path, version: str):
        self.path = path
        self.version = version
        self.files: Dict[str, str] = {}  # relative_path -> hash
        self.metadata: Dict = {}
        self._capture()
    
    def _capture(self):
        """Capture current state of template."""
        for file_path in self.path.rglob('*'):
            if file_path.is_file() and not self._should_ignore(file_path):
                rel_path = str(file_path.relative_to(self.path))
                self.files[rel_path] = self._hash_file(file_path)
                
                # Capture metadata for specific files
                if file_path.name == 'values.yaml':
                    with open(file_path, 'r') as f:
                        self.metadata['values'] = yaml.safe_load(f)
                elif file_path.name == 'Chart.yaml':
                    with open(file_path, 'r') as f:
                        self.metadata['chart'] = yaml.safe_load(f)
    
    def _should_ignore(self, path: Path) -> bool:
        """Check if path should be ignored."""
        ignore_patterns = [
            '.git', '__pycache__', '.pyc', '.pyo',
            'Chart.lock', '*.tgz', 'charts/',
            '.gitignore',  # Add .gitignore to ignored files
            '.policystack',  # Ignore our metadata directory
        ]
        path_str = str(path)
        
        # Check patterns
        for pattern in ignore_patterns:
            if '*' in pattern:
                # Handle wildcards
                import fnmatch
                if fnmatch.fnmatch(path.name, pattern):
                    return True
            elif pattern in path_str:
                return True
        
        return False
 
    def _hash_file(self, path: Path) -> str:
        """Calculate file hash."""
        hasher = hashlib.sha256()
        with open(path, 'rb') as f:
            hasher.update(f.read())
        return hasher.hexdigest()
    
    def save(self, snapshot_path: Path):
        """Save snapshot to file."""
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            'version': self.version,
            'files': self.files,
            'metadata': self.metadata
        }
        with open(snapshot_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    @classmethod
    def load(cls, snapshot_path: Path) -> 'TemplateSnapshot':
        """Load snapshot from file."""
        with open(snapshot_path, 'r') as f:
            data = json.load(f)
        
        snapshot = cls.__new__(cls)
        snapshot.version = data['version']
        snapshot.files = data['files']
        snapshot.metadata = data['metadata']
        snapshot.path = snapshot_path.parent
        return snapshot


class ChangeDetector:
    """Detects changes between template versions."""
    
    def __init__(self, element_path: Path):
        self.element_path = element_path
        self.snapshot_dir = element_path / '.policystack' / 'snapshots'
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
    
    def capture_baseline(self, version: str) -> TemplateSnapshot:
        """Capture baseline snapshot after installation."""
        snapshot = TemplateSnapshot(self.element_path, version)
        snapshot.save(self.snapshot_dir / 'baseline.json')
        return snapshot
    
    def detect_changes(self) -> Dict[str, List[FileChange]]:
        """Detect all changes from baseline."""
        baseline_path = self.snapshot_dir / 'baseline.json'
        if not baseline_path.exists():
            raise ValueError("No baseline snapshot found. Was this template properly installed?")
        
        baseline = TemplateSnapshot.load(baseline_path)
        current = TemplateSnapshot(self.element_path, "current")
        
        changes = {
            'added': [],
            'modified': [],
            'deleted': [],
            'unchanged': []
        }
        
        # Check for modifications and deletions
        for rel_path, baseline_hash in baseline.files.items():
            if rel_path in current.files:
                if current.files[rel_path] != baseline_hash:
                    change = FileChange(
                        self.element_path / rel_path,
                        ChangeType.MODIFIED
                    )
                    change.original_hash = baseline_hash
                    change.current_hash = current.files[rel_path]
                    changes['modified'].append(change)
                else:
                    changes['unchanged'].append(
                        FileChange(self.element_path / rel_path, ChangeType.UNCHANGED)
                    )
            else:
                changes['deleted'].append(
                    FileChange(self.element_path / rel_path, ChangeType.DELETED)
                )
        
        # Check for additions
        for rel_path in current.files:
            if rel_path not in baseline.files:
                changes['added'].append(
                    FileChange(self.element_path / rel_path, ChangeType.ADDED)
                )
        
        return changes
    
    def detect_values_changes(self) -> Dict:
        """Detect specific changes in values.yaml."""
        baseline_path = self.snapshot_dir / 'baseline.json'
        if not baseline_path.exists():
            return {}
        
        baseline = TemplateSnapshot.load(baseline_path)
        values_path = self.element_path / 'values.yaml'
        
        if not values_path.exists():
            return {'error': 'values.yaml not found'}
        
        with open(values_path, 'r') as f:
            current_values = yaml.safe_load(f)
        
        baseline_values = baseline.metadata.get('values', {})
        
        # Use DeepDiff for detailed comparison
        diff = DeepDiff(
            baseline_values,
            current_values,
            ignore_order=True,
            verbose_level=2
        )
        
        return {
            'added': diff.get('dictionary_item_added', []),
            'removed': diff.get('dictionary_item_removed', []),
            'changed': diff.get('values_changed', {}),
            'type_changes': diff.get('type_changes', {})
        }
