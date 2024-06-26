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
    # link_hash is not passed in but rather computed as the hash of the tuple of hub_hashes
    created_ts: datetime
    record_source: str
    hub_hashes: List[str]
    link_hash: str = None