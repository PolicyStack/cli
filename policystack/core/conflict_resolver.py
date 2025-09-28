"""Conflict resolution system for PolicyStack."""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import yaml
import copy
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from ruamel.yaml import YAML

from .merger import MergeConflict


class ConflictReport:
    """Represents a complete conflict report for an upgrade."""
    
    def __init__(self, from_version: str, to_version: str, element_name: str):
        self.from_version = from_version
        self.to_version = to_version
        self.element_name = element_name
        self.timestamp = datetime.now().isoformat()
        self.conflicts: Dict[str, List[MergeConflict]] = {
            'values.yaml': [],
            'converters': [],
            'other': []
        }
        self.resolutions: Dict[str, str] = {}
        self.metadata: Dict = {}
    
    def add_conflict(self, file_path: str, conflict: MergeConflict):
        """Add a conflict to the report."""
        if 'values.yaml' in file_path:
            self.conflicts['values.yaml'].append(conflict)
        elif 'converters' in file_path:
            self.conflicts['converters'].append(conflict)
        else:
            self.conflicts['other'].append(conflict)
    
    def save(self, path: Path):
        """Save report to file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            'upgrade': {
                'from': self.from_version,
                'to': self.to_version,
                'element': self.element_name,
                'timestamp': self.timestamp
            },
            'conflicts': self._serialize_conflicts(),
            'resolutions': self.resolutions,
            'metadata': self.metadata
        }
        
        with open(path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    
    def _serialize_conflicts(self) -> Dict:
        """Serialize conflicts for saving."""
        serialized = {}
        for category, conflicts in self.conflicts.items():
            serialized[category] = []
            for conflict in conflicts:
                serialized[category].append({
                    'path': conflict.path,
                    'base': conflict.base_value,
                    'local': conflict.local_value,
                    'remote': conflict.remote_value,
                    'auto_resolvable': conflict.auto_resolvable,
                    'resolution': conflict.resolution
                })
        return serialized
    
    @classmethod
    def load(cls, path: Path) -> 'ConflictReport':
        """Load report from file."""
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        report = cls(
            data['upgrade']['from'],
            data['upgrade']['to'],
            data['upgrade']['element']
        )
        report.timestamp = data['upgrade']['timestamp']
        
        # Reconstruct conflicts
        for category, conflicts in data['conflicts'].items():
            for conflict_data in conflicts:
                conflict = MergeConflict(
                    conflict_data['path'],
                    conflict_data['base'],
                    conflict_data['local'],
                    conflict_data['remote']
                )
                conflict.auto_resolvable = conflict_data.get('auto_resolvable', False)
                conflict.resolution = conflict_data.get('resolution')
                report.conflicts[category].append(conflict)
        
        report.resolutions = data.get('resolutions', {})
        report.metadata = data.get('metadata', {})
        
        return report
    
    def has_conflicts(self) -> bool:
        """Check if there are any unresolved conflicts."""
        for conflicts in self.conflicts.values():
            for conflict in conflicts:
                if not conflict.resolution:
                    return True
        return False
    
    def get_unresolved_count(self) -> int:
        """Get count of unresolved conflicts."""
        count = 0
        for conflicts in self.conflicts.values():
            count += sum(1 for c in conflicts if not c.resolution)
        return count


class ConflictResolver:
    """Interactive conflict resolution."""
    
    def __init__(self, console: Console):
        self.console = console
    
    def resolve_interactively(self, report: ConflictReport) -> ConflictReport:
        """Interactively resolve conflicts."""
        self.console.print(
            Panel.fit(
                f"[bold]Merge Conflicts Found[/bold]\n"
                f"Upgrading {report.element_name}: {report.from_version} → {report.to_version}\n"
                f"Total conflicts: {report.get_unresolved_count()}",
                border_style="yellow"
            )
        )
        
        # Auto-resolve what is possible
        auto_resolved = self._auto_resolve(report)
        if auto_resolved > 0:
            self.console.print(f"\n[green]✓ Auto-resolved {auto_resolved} conflicts[/green]")
        
        # Handle remaining conflicts
        remaining = report.get_unresolved_count()
        if remaining > 0:
            self.console.print(f"\n[yellow]⚠ {remaining} conflicts require manual resolution[/yellow]\n")
            
            # Resolve values.yaml conflicts
            if report.conflicts['values.yaml']:
                self._resolve_category(report, 'values.yaml', "Configuration Values")
            
            # Resolve converter conflicts
            if report.conflicts['converters']:
                self._resolve_category(report, 'converters', "Converter Templates")
            
            # Resolve other conflicts
            if report.conflicts['other']:
                self._resolve_category(report, 'other', "Other Files")
        
        return report
    
    def _auto_resolve(self, report: ConflictReport) -> int:
        """Auto-resolve conflicts where possible."""
        count = 0
        for conflicts in report.conflicts.values():
            for conflict in conflicts:
                if conflict.auto_resolvable and conflict.resolution:
                    count += 1
        return count
    
    def _resolve_category(self, report: ConflictReport, category: str, display_name: str):
        """Resolve conflicts in a category."""
        conflicts = [c for c in report.conflicts[category] if not c.resolution]
        if not conflicts:
            return
        
        self.console.print(f"\n[bold]{display_name} Conflicts[/bold]")
        
        for i, conflict in enumerate(conflicts, 1):
            self.console.print(f"\n[cyan]Conflict {i}/{len(conflicts)}: {conflict.path}[/cyan]")
            
            # Create comparison table
            table = Table(show_header=True, header_style="bold")
            table.add_column("Version", style="bold")
            table.add_column("Value")
            
            # Format values for display
            base_str = self._format_value(conflict.base_value)
            local_str = self._format_value(conflict.local_value)
            remote_str = self._format_value(conflict.remote_value)
            
            table.add_row("Base (Original)", base_str)
            table.add_row("[green]Local (Your Changes)[/green]", local_str)
            table.add_row("[blue]Remote (New Version)[/blue]", remote_str)
            
            self.console.print(table)
            
            # Get resolution
            resolution = self._get_resolution(conflict)
            conflict.resolution = resolution
            report.resolutions[conflict.path] = resolution
            
            self.console.print(f"[dim]Resolved: {resolution}[/dim]\n")
    
    def _format_value(self, value: any) -> str:
        """Format value for display."""
        if value is None:
            return "[dim](not present)[/dim]"
        elif isinstance(value, (dict, list)):
            return yaml.dump(value, default_flow_style=False, sort_keys=False)
        else:
            return str(value)
    
    def _get_resolution(self, conflict: MergeConflict) -> str:
        """Get resolution choice from user."""
        choices = {
            '1': 'keep_local',
            '2': 'take_remote',
            '3': 'manual',
            'l': 'keep_local',
            'r': 'take_remote',
            'm': 'manual'
        }
        
        while True:
            choice = Prompt.ask(
                "Resolution",
                choices=['1', '2', '3', 'l', 'r', 'm'],
                default='1'
            )
            
            if choice in choices:
                return choices[choice]

class ConflictMarkerGenerator:
    """Generate conflict markers in files."""
    
    @staticmethod  
    def apply_resolution_to_values(
        values: Any,
        conflicts: List[MergeConflict], 
        yaml_handler: YAML
    ) -> Any:
        """Apply conflict resolutions to values structure."""
        result = copy.deepcopy(values)
        
        for conflict in conflicts:
            if conflict.resolution:
                # Navigate to the conflict path and apply resolution
                path_parts = conflict.path.replace('[', '.').replace(']', '').split('.')
                current = result
                
                # Navigate to parent
                for part in path_parts[:-1]:
                    if part.startswith('name='):
                        # Handle named list items
                        name = part.split('=')[1]
                        for item in current:
                            if isinstance(item, dict) and item.get('name') == name:
                                current = item
                                break
                    else:
                        if part in current:
                            current = current[part]
                
                # Apply resolution
                last_key = path_parts[-1]
                if conflict.resolution == 'keep_local':
                    current[last_key] = conflict.local_value
                elif conflict.resolution == 'take_remote':
                    current[last_key] = conflict.remote_value
                # For manual resolution, keep local by default
        
        return result

