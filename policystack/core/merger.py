"""Three-way merge system for PolicyStack templates with comment preservation."""

import copy
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from deepdiff import DeepDiff

class MergeConflict:
    """Represents a merge conflict."""
    
    def __init__(self, path: str, base_value: Any, local_value: Any, remote_value: Any):
        self.path = path
        self.base_value = base_value
        self.local_value = local_value
        self.remote_value = remote_value
        self.resolution: Optional[str] = None
        self.auto_resolvable = self._check_auto_resolvable()
    
    def _check_auto_resolvable(self) -> bool:
        """Check if conflict can be auto-resolved."""
        # If local changed and remote didn't, keep local
        if self.base_value == self.remote_value:
            self.resolution = 'keep_local'
            return True
        # If remote changed and local didn't, take remote  
        if self.base_value == self.local_value:
            self.resolution = 'take_remote'
            return True
        return False

class PreservingYAMLMerger:
    """Three-way YAML merger that preserves comments and order."""
    
    def __init__(self):
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.width = 4096  # Prevent line wrapping
        self.yaml.indent(mapping=2, sequence=2, offset=0)
        self.conflicts: List[MergeConflict] = []
    
    def load_yaml_preserving(self, path: Path) -> CommentedMap:
        """Load YAML while preserving comments and structure."""
        if not path.exists():
            return CommentedMap()
        
        with open(path, 'r') as f:
            return self.yaml.load(f) or CommentedMap()
    
    def merge(self, base: Any, local: Any, remote: Any) -> Tuple[Any, List[MergeConflict]]:
        """Perform three-way merge preserving structure."""
        self.conflicts = []
        
        # If local and remote are CommentedMaps, preserve the local's comments
        merged = self._deep_merge(base, local, remote, path="")
        
        # Preserve comments from local version where possible
        if isinstance(local, CommentedMap) and isinstance(merged, CommentedMap):
            self._preserve_comments(local, merged)
        
        return merged, self.conflicts
    
    def _preserve_comments(self, source: CommentedMap, target: CommentedMap):
        """Preserve comments from source in target."""
        if not isinstance(source, CommentedMap) or not isinstance(target, CommentedMap):
            return
    
        # Copy comment attributes properly
        if hasattr(source, '_yaml_comment'):
            target._yaml_comment = source._yaml_comment
    
        # Copy individual comment attributes
        for attr in ['comment', 'yaml_set_comment_before_after_key']:
            if hasattr(source, attr):
                try:
                    setattr(target, attr, getattr(source, attr))
                except (AttributeError, TypeError):
                    pass
    
        # Recursively preserve for nested structures
        for key in target:
            if key in source:
                if isinstance(source[key], CommentedMap) and isinstance(target[key], CommentedMap):
                    self._preserve_comments(source[key], target[key])
 
    def _deep_merge(self, base: Any, local: Any, remote: Any, path: str) -> Any:
        """Recursively merge nested structures preserving types."""
        # If all three are equal, return local to preserve comments
        if local == remote:
            return local
        
        # Handle CommentedMap/dict
        if isinstance(local, (dict, CommentedMap)) and isinstance(remote, (dict, CommentedMap)):
            return self._merge_dicts(base or CommentedMap(), local, remote, path)
        
        # Handle CommentedSeq/list
        if isinstance(local, (list, CommentedSeq)) and isinstance(remote, (list, CommentedSeq)):
            return self._merge_lists(base or CommentedSeq(), local, remote, path)
        
        # If types differ or scalar values differ
        if local != remote:
            if base == local:
                return remote  # Remote changed
            elif base == remote:
                return local   # Local changed
            else:
                # Both changed - conflict
                conflict = MergeConflict(path, base, local, remote)
                self.conflicts.append(conflict)
                return local  # Default to local
        
        return local
    
    def _merge_dicts(self, base: Dict, local: Dict, remote: Dict, path: str) -> CommentedMap:
        """Merge dictionary structures preserving order."""
        # Start with local's structure to preserve order and comments
        if isinstance(local, CommentedMap):
            merged = CommentedMap()
            # Preserve local's key order
            for key in local:
                merged[key] = None  # Placeholder
        else:
            merged = CommentedMap()
        
        all_keys = list(local.keys())
        # Add remote keys not in local, maintaining order
        for key in remote:
            if key not in all_keys:
                all_keys.append(key)
        
        for key in all_keys:
            key_path = f"{path}.{key}" if path else str(key)
            
            if key in local and key in remote:
                base_value = base.get(key) if base else None
                merged[key] = self._deep_merge(base_value, local[key], remote[key], key_path)
            elif key in local:
                # Local only - keep it
                merged[key] = local[key]
                if key in base:
                    # Was deleted in remote
                    conflict = MergeConflict(key_path, base[key], local[key], None)
                    self.conflicts.append(conflict)
            else:
                # Remote only - add it
                merged[key] = remote[key]
        
        return merged
    
    def _merge_lists(self, base: List, local: List, remote: List, path: str) -> CommentedSeq:
        """Merge list structures."""
        # Check if it's a named list
        if self._is_named_list(local) or self._is_named_list(remote):
            return self._merge_named_lists(base, local, remote, path)
        
        # For simple lists, if both changed differently, it's a conflict
        if local != base and remote != base and local != remote:
            conflict = MergeConflict(path, base, local, remote)
            self.conflicts.append(conflict)
            return local if isinstance(local, CommentedSeq) else CommentedSeq(local)
        
        # Take whichever changed
        if local != base:
            return local if isinstance(local, CommentedSeq) else CommentedSeq(local)
        return remote if isinstance(remote, CommentedSeq) else CommentedSeq(remote)
    
    def _is_named_list(self, lst: List) -> bool:
        """Check if list contains named items."""
        return (
            lst and 
            all(isinstance(item, (dict, CommentedMap)) for item in lst) and
            all('name' in item for item in lst)
        )
    
    def _merge_named_lists(self, base: List, local: List, remote: List, path: str) -> CommentedSeq:
        """Merge lists of named items preserving order."""
        merged = CommentedSeq()
        
        # Index items by name
        base_by_name = {item.get('name'): item for item in (base or []) 
                       if isinstance(item, (dict, CommentedMap))}
        local_by_name = {item.get('name'): item for item in local 
                        if isinstance(item, (dict, CommentedMap))}
        remote_by_name = {item.get('name'): item for item in remote 
                         if isinstance(item, (dict, CommentedMap))}
        
        # Preserve local order, add remote additions at the end
        seen_names = set()
        
        # First, add all local items (preserving order)
        for item in local:
            if isinstance(item, (dict, CommentedMap)):
                name = item.get('name')
                if name:
                    seen_names.add(name)
                    if name in remote_by_name:
                        # Merge with remote
                        base_item = base_by_name.get(name, CommentedMap())
                        merged_item = self._deep_merge(
                            base_item, item, remote_by_name[name], 
                            f"{path}[name={name}]"
                        )
                        merged.append(merged_item)
                    else:
                        # Local only
                        merged.append(item)
        
        # Add remote-only items
        for name, item in remote_by_name.items():
            if name not in seen_names:
                merged.append(item)
        
        return merged

class HelmTemplateMerger:
    """Special merger for Helm template files."""
    
    @staticmethod
    def merge_template(base: str, local: str, remote: str) -> Tuple[str, List[MergeConflict]]:
        """Merge Helm template files preserving template syntax."""
        conflicts = []
        
        # Parse template blocks
        def extract_blocks(content):
            """Extract template blocks and static content."""
            blocks = []
            pattern = r'({{[^}]*}})'
            parts = re.split(pattern, content)
            
            for i, part in enumerate(parts):
                if i % 2 == 0:  # Static content
                    if part.strip():
                        blocks.append(('static', part))
                else:  # Template block
                    blocks.append(('template', part))
            return blocks
        
        base_blocks = extract_blocks(base)
        local_blocks = extract_blocks(local)
        remote_blocks = extract_blocks(remote)
        
        # Smart merge of blocks
        # This is simplified - real implementation would be more sophisticated
        merged = []
        
        # If structures are similar, merge block by block
        if len(local_blocks) == len(remote_blocks):
            for i, (base_block, local_block, remote_block) in enumerate(
                zip(base_blocks, local_blocks, remote_blocks)
            ):
                if local_block == remote_block:
                    merged.append(local_block[1])
                elif local_block == base_block:
                    merged.append(remote_block[1])
                elif remote_block == base_block:
                    merged.append(local_block[1])
                else:
                    # Conflict - need manual resolution
                    conflict = MergeConflict(
                        f"block_{i}",
                        base_block[1] if i < len(base_blocks) else None,
                        local_block[1],
                        remote_block[1]
                    )
                    conflicts.append(conflict)
                    # Add conflict marker
                    merged.append(f"""
{{- /* MERGE CONFLICT START */ -}}
{{- /* LOCAL VERSION: */ -}}
{local_block[1]}
{{- /* REMOTE VERSION: */ -}}
{remote_block[1]}
{{- /* MERGE CONFLICT END */ -}}""")
        else:
            # Structure changed significantly, mark whole file as conflict
            conflicts.append(MergeConflict("entire_file", base, local, remote))
            return local, conflicts  # Keep local by default
        
        return ''.join(merged), conflicts


class YAMLMerger:
    """Three-way YAML merger."""
    
    def __init__(self):
        self.conflicts: List[MergeConflict] = []
    
    def merge(self, base: Dict, local: Dict, remote: Dict) -> Tuple[Dict, List[MergeConflict]]:
        """Perform three-way merge on YAML dictionaries."""
        self.conflicts = []
        merged = self._deep_merge(base, local, remote, path="")
        return merged, self.conflicts
    
    def _deep_merge(self, base: Any, local: Any, remote: Any, path: str) -> Any:
        """Recursively merge nested structures."""
        # If all three are equal, no change needed
        if local == remote:
            return local
        
        # If types differ, conflict
        if type(local) != type(remote):
            if base == local:
                return remote  # Take remote change
            elif base == remote:
                return local   # Keep local change
            else:
                # Both changed differently - conflict
                conflict = MergeConflict(path, base, local, remote)
                self.conflicts.append(conflict)
                return local  # Default to local
        
        # Merge dictionaries
        if isinstance(local, dict) and isinstance(remote, dict):
            return self._merge_dicts(base or {}, local, remote, path)
        
        # Merge lists
        if isinstance(local, list) and isinstance(remote, list):
            return self._merge_lists(base or [], local, remote, path)
        
        # Scalar values
        if local != remote:
            if base == local:
                return remote  # Remote changed
            elif base == remote:
                return local   # Local changed
            else:
                # Both changed - conflict
                conflict = MergeConflict(path, base, local, remote)
                self.conflicts.append(conflict)
                return local  # Default to local
        
        return local
    
    def _merge_dicts(self, base: Dict, local: Dict, remote: Dict, path: str) -> Dict:
        """Merge dictionary structures."""
        merged = {}
        all_keys = set(local.keys()) | set(remote.keys())
        
        for key in all_keys:
            key_path = f"{path}.{key}" if path else key
            
            # Key exists in all three
            if key in local and key in remote:
                base_value = base.get(key)
                merged[key] = self._deep_merge(base_value, local[key], remote[key], key_path)
            
            # Key only in local (local addition)
            elif key in local:
                if key in base:
                    # Deleted in remote, but have local changes - conflict
                    conflict = MergeConflict(key_path, base[key], local[key], None)
                    self.conflicts.append(conflict)
                merged[key] = local[key]
            
            # Key only in remote (remote addition)
            else:
                if key in base:
                    # Deleted in local, but remote has changes - conflict
                    conflict = MergeConflict(key_path, base[key], None, remote[key])
                    self.conflicts.append(conflict)
                else:
                    # New in remote, add it
                    merged[key] = remote[key]
        
        return merged
    
    def _merge_lists(self, base: List, local: List, remote: List, path: str) -> List:
        """Merge list structures - this is complex for Helm values."""
        # For PolicyStack, lists often contain policy definitions
        # use a smart merge based on 'name' field if present
        
        if self._is_named_list(local) or self._is_named_list(remote):
            return self._merge_named_lists(base, local, remote, path)
        
        # For simple lists, if both changed, it's a conflict
        if local != base and remote != base and local != remote:
            conflict = MergeConflict(path, base, local, remote)
            self.conflicts.append(conflict)
            return local
        
        # Take whichever changed
        if local != base:
            return local
        return remote
    
    def _is_named_list(self, lst: List) -> bool:
        """Check if list contains named items (dicts with 'name' field)."""
        return (
            lst and 
            all(isinstance(item, dict) for item in lst) and
            all('name' in item for item in lst)
        )
    
    def _merge_named_lists(self, base: List, local: List, remote: List, path: str) -> List:
        """Merge lists of named items (like policies)."""
        merged = []
        
        # Index items by name
        base_by_name = {item.get('name'): item for item in base if isinstance(item, dict)}
        local_by_name = {item.get('name'): item for item in local if isinstance(item, dict)}
        remote_by_name = {item.get('name'): item for item in remote if isinstance(item, dict)}
        
        all_names = set(local_by_name.keys()) | set(remote_by_name.keys())
        
        for name in all_names:
            item_path = f"{path}[name={name}]"
            
            base_item = base_by_name.get(name, {})
            local_item = local_by_name.get(name)
            remote_item = remote_by_name.get(name)
            
            if local_item and remote_item:
                # Both have it, merge recursively
                merged_item = self._deep_merge(base_item, local_item, remote_item, item_path)
                merged.append(merged_item)
            elif local_item:
                # Only in local
                merged.append(local_item)
            else:
                # Only in remote
                merged.append(remote_item)
        
        return merged
