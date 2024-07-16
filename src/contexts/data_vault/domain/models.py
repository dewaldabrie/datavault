from dataclasses import dataclass, field
from typing import List, Dict, Any
from datetime import datetime
from src.contexts.root.common import HASH_LENGTH
from src.contexts.root.domain.models import ColumnSchema
from src.contexts.root.common import calculate_hash_key, calculate_hash_diff


@dataclass
class HubSchema:
    hub_name: str
    business_key: ColumnSchema = field(default_factory=lambda: ColumnSchema(name='business_key', type=str, type_length=100))
    columns: List[ColumnSchema] = field(default_factory=lambda: [
        ColumnSchema(name='hash_key', type=str, primary_key=True, type_length=HASH_LENGTH, nullable=False, unique=True),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500)
    ])

@dataclass
class HubDataBase:
    business_key: str
    created_ts: datetime
    record_source: str
    hash_key: str = field(init=False)


class HubData(HubDataBase):
    """
    If we override the hash_key like this with a property, 
    it still gets included in dataclass.asdict
    """
    @property
    def hash_key(self) -> str:
        return calculate_hash_key(self.business_key)

@dataclass
class XDomainHubData(HubDataBase):
    """
    Use this if your business keys come from different domains.
    I.e. business keys come from systems that have no common id.
    """
    @property
    def hash_key(self) -> str:
        return calculate_hash_key((self.record_source or '') + '|' + self.business_key)


@dataclass
class SatelliteSchema:
    sat_name: str
    hub_name: str
    columns: List[ColumnSchema] = field(default_factory=lambda: [
        ColumnSchema(name='hub_hash_key', type=str, primary_key=True, foreign_key='', type_length=HASH_LENGTH, nullable=False, unique=True),
        ColumnSchema(name='created_ts', type=datetime, primary_key=True, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500),
        ColumnSchema(name='attributes', type=dict),
        ColumnSchema(name='hash_diff', type=str, type_length=HASH_LENGTH)
    ])

@dataclass
class DocumentSatelliteDataBase:
    """
    The "Document" sattelite avoids having custom fields per table
    by putting all non-system-fields into an attributes dictionary which
    should go into DB JSON columns.
    """
    hub_hash_key: str
    created_ts: datetime
    record_source: str
    attributes: Dict[str, Any]
    hash_diff: str = field(init=False)

class DocumentSatelliteData(DocumentSatelliteDataBase):
    """
    If we override the hash_key like this with a property, 
    it still gets included in dataclass.asdict
    """
    @property
    def hash_diff(self) -> str:
        return calculate_hash_diff(self.attributes)
    

@dataclass
class LinkSchema:
    link_name: str
    hub_names: List[str]
    additional_columns: List[ColumnSchema] = field(default_factory=list)
    columns: List[ColumnSchema] = field(default_factory=lambda: [
        ColumnSchema(name='hash_key', type=str, primary_key=True, type_length=HASH_LENGTH, nullable=False, unique=True),
        ColumnSchema(name='created_ts', type=datetime, default=datetime.utcnow),
        ColumnSchema(name='record_source', type=str, type_length=500)
    ])

@dataclass
class LinkDataBase:
    created_ts: datetime
    record_source: str
    hub_hashes: Dict[str, str]  # key: hub_name, val: hub_hash_key
    hash_key: str = field(init=False)

class LinkData(LinkDataBase):
    """
    If we override the hash_key like this with a property, 
    it still gets included in dataclass.asdict
    """
    @property
    def hash_key(self) -> str:
        return calculate_hash_key(self.hub_hashes, self.created_ts)