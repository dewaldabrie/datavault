from dataclasses import dataclass, field
from typing import List, Dict, Any
from datetime import datetime
from src.contexts.root.common import HASH_LENGTH
from src.contexts.root.domain.models import ColumnSchema


@dataclass
class HubSchema:
    hub_name: str
    business_key: ColumnSchema = field(default_factory=lambda: ColumnSchema(name='business_key', type=str, type_length=100))
    columns: List[ColumnSchema] = field(default_factory=lambda: [
        ColumnSchema(name='hub_hash', type=str, primary_key=True, type_length=HASH_LENGTH, nullable=False, unique=True),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500)
    ])

@dataclass
class HubData:
    business_key: str
    created_ts: datetime
    record_source: str
    hub_hash: str = None

@dataclass
class SatelliteSchema:
    sat_name: str
    hub_name: str
    columns: List[ColumnSchema] = field(default_factory=lambda: [
        ColumnSchema(name='hub_hash', type=str, primary_key=True, foreign_key='', type_length=HASH_LENGTH, nullable=False, unique=True),
        ColumnSchema(name='created_ts', type=datetime, primary_key=True, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500),
        ColumnSchema(name='attributes', type=dict),
        ColumnSchema(name='hash_diff', type=str, type_length=HASH_LENGTH)
    ])

@dataclass
class SatelliteData:
    hub_hash: str
    created_ts: datetime
    record_source: str
    attributes: Dict[str, Any]
    hash_diff: str = None

@dataclass
class LinkSchema:
    link_name: str
    hub_names: List[str]
    additional_columns: List[ColumnSchema] = field(default_factory=list)
    columns: List[ColumnSchema] = field(default_factory=lambda: [
        ColumnSchema(name='link_hash', type=str, primary_key=True, type_length=HASH_LENGTH, nullable=False, unique=True),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500)
    ])

@dataclass
class LinkData:
    created_ts: datetime
    record_source: str
    hub_hashes: Dict[str, str]  # key: hub_name, val: hub_hash_key
    link_hash: str = None