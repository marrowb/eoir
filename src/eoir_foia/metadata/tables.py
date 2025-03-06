"""Table definitions."""
from dataclasses import dataclass
from typing import List

@dataclass
class Column:
    name: str
    type: str
    nullable: bool = True

@dataclass
class Table:
    name: str
    columns: List[Column]
