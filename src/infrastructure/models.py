from sqlmodel import SQLModel, Field
from typing import Any, Optional, List
from datetime import datetime

# I think we don't need the rest ...

class ColumnSchema(SQLModel, table=False):
    name: str = Field(primary_key=True)
    type: str
    type_length: Optional[int] = None
    description: Optional[str] = None
    default: Any = None
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    foreign_key: Optional[str] = None
