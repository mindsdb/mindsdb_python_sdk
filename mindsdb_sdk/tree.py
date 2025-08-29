from typing import List, Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class TreeNode:
    """Represents any node in the MindsDB tree structure."""
    name: str
    class_: str  # 'class' is a reserved keyword, so using 'class_'
    type: Optional[str] = None
    engine: Optional[str] = None
    deletable: bool = False
    visible: bool = True
    schema: Optional[str] = None  # For table nodes that have schema information
    children: Optional[List['TreeNode']] = None
    
    def __post_init__(self):
        if self.children is None:
            self.children = []
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TreeNode':
        """Create TreeNode from dictionary data."""
        children = []
        if 'children' in data and data['children']:
            children = [cls.from_dict(child) for child in data['children']]
        
        return cls(
            name=data['name'],
            class_=data.get('class', ''),
            type=data.get('type'),
            engine=data.get('engine'),
            deletable=data.get('deletable', False),
            visible=data.get('visible', True),
            schema=data.get('schema'),  # Include schema if present
            children=children
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert TreeNode to dictionary."""
        result = {
            'name': self.name,
            'class': self.class_,
            'deletable': self.deletable,
            'visible': self.visible
        }
        
        if self.type is not None:
            result['type'] = self.type
        if self.engine is not None:
            result['engine'] = self.engine
        if self.schema is not None:
            result['schema'] = self.schema
        if self.children:
            result['children'] = [child.to_dict() for child in self.children]
            
        return result
