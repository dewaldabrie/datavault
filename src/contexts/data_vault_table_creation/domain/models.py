from typing import List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class ColumnSchema(BaseModel):
    name: str
    type: Any
    primary_key: bool = False
    default: Any = None
    foreign_key: str = None

class HubSchema(BaseModel):
    hub_name: str
    business_key: str
    columns: List[ColumnSchema] = Field(default_factory=lambda: [
        ColumnSchema(name='hub_hash', type=str, primary_key=True),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str)
    ])

class SatelliteSchema(BaseModel):
    sat_name: str
    hub_name: str
    columns: List[ColumnSchema] = Field(default_factory=lambda: [
        ColumnSchema(name='hub_hash', type=str, primary_key=True, foreign_key=''),
        ColumnSchema(name='created_ts', type=datetime, primary_key=True, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str),
        ColumnSchema(name='attributes', type=dict),
        ColumnSchema(name='hash_diff', type=str)
    ])

class LinkSchema(BaseModel):
    link_name: str
    hub_names: List[str]
    additional_columns: List[ColumnSchema] = Field(default_factory=list)
    columns: List[ColumnSchema] = Field(default_factory=lambda: [
        ColumnSchema(name='link_hash', type=str, primary_key=True),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str)
    ])