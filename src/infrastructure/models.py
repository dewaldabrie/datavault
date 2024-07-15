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

class HubSchema(SQLModel, table=False):
    hub_name: str = Field(primary_key=True)
    business_key: str
    columns: List[ColumnSchema]

class SatelliteSchema(SQLModel, table=False):
    sat_name: str = Field(primary_key=True)
    hub_name: str
    columns: List[ColumnSchema]

class LinkSchema(SQLModel, table=False):
    link_name: str = Field(primary_key=True)
    hub_names: List[str]
    additional_columns: List[ColumnSchema]
    columns: List[ColumnSchema]

class HubData(SQLModel, table=False):
    business_key: str
    created_ts: datetime
    record_source: str
    hub_hash: Optional[str] = None

class SatelliteData(SQLModel, table=False):
    hub_hash: str
    created_ts: datetime
    record_source: str
    attributes: dict
    hash_diff: Optional[str] = None

class LinkData(SQLModel, table=False):
    created_ts: datetime
    record_source: str
    hub_hashes: List[str]
    link_hash: Optional[str] = None