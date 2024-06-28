from typing import List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from src.contexts.root.common import HASH_LENGTH
from src.contexts.root.domain.models import ColumnSchema


class HubSchema(BaseModel):
    hub_name: str
    business_key: ColumnSchema = Field(default_factory=lambda:ColumnSchema(name='business_key', type=str, type_length=100))
    columns: List[ColumnSchema] = Field(default_factory=lambda: [
        ColumnSchema(name='hub_hash', type=str, primary_key=True, type_length=HASH_LENGTH),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500)
    ])

class HubData(BaseModel):
    business_key: str
    created_ts: datetime
    record_source: str
    hub_hash: str = None


class SatelliteSchema(BaseModel):
    sat_name: str
    hub_name: str
    columns: List[ColumnSchema] = Field(default_factory=lambda: [
        ColumnSchema(name='hub_hash', type=str, primary_key=True, foreign_key='', type_length=HASH_LENGTH),
        ColumnSchema(name='created_ts', type=datetime, primary_key=True, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500),
        ColumnSchema(name='attributes', type=dict),
        ColumnSchema(name='hash_diff', type=str, type_length=HASH_LENGTH)
    ])


class SatelliteData(BaseModel):
    hub_hash: str
    created_ts: datetime
    record_source: str
    attributes: Dict[str, Any]
    hash_diff: str = None


class LinkSchema(BaseModel):
    link_name: str
    hub_names: List[str]
    additional_columns: List[ColumnSchema] = Field(default_factory=list)
    columns: List[ColumnSchema] = Field(default_factory=lambda: [
        ColumnSchema(name='link_hash', type=str, primary_key=True, type_length=HASH_LENGTH),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500)
    ])
    

class LinkData(BaseModel):
    # link_hash is not passed in but rather computed as the hash of the tuple of hub_hashes
    created_ts: datetime
    record_source: str
    hub_hashes: List[str]
    link_hash: str = None