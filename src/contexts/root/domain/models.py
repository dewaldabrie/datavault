from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnSchema:
    name: str
    type: Any
    primary_key: bool = False
    default: Any = None
    nullable: bool = True
    unique: bool = False
    foreign_key: str = None
    type_length: int = None
    description: str = None
