from typing import Any, List
from pydantic import BaseModel


class ColumnSchema(BaseModel):
    name: str
    type: Any
    primary_key: bool = False
    default: Any = None
    nullable: bool = True
    unique: bool = False
    foreign_key: str = None
    type_length: int = None
    description: str = None