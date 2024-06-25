from typing import List, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class HubData(BaseModel):
    business_key: str
    created_ts: datetime
    record_source: str
    hub_hash: str = None

class SatelliteData(BaseModel):
    hub_hash: str
    created_ts: datetime
    record_source: str
    attributes: Dict[str, Any]
    hash_diff: str = None

class LinkData(BaseModel):
    link_hash: str
    created_ts: datetime
    record_source: str
    hub_hashes: List[str]