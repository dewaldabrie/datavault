from sqlalchemy import Column, String, DateTime, JSON, ForeignKey
from datetime import datetime
from typing import List, Dict, Any
from src.domain.interfaces import DatabaseHandler

class DataVaultTableCreation:
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler

    def create_hub(self, hub_name: str, business_key: str):
        columns = [
            Column('hub_hash', String(32), primary_key=True),
            Column('created_ts', DateTime, default=datetime.utcnow),
            Column('record_source', String(50)),
            Column(business_key, String(50))
        ]
        self.db_handler.create_table(hub_name, columns)

    def create_satelite(self, sat_name: str, hub_name: str):
        columns = [
            Column('hub_hash', String(32), ForeignKey(f'{hub_name}.hub_hash'), primary_key=True),
            Column('created_ts', DateTime, primary_key=True, default=datetime.utcnow),
            Column('record_source', String(50)),
            Column('attributes', JSON),
            Column('hash_diff', String(32))
        ]
        self.db_handler.create_table(sat_name, columns)

    def create_link(self, link_name: str, hub_names: List[str], additional_columns: List[Dict[str, Any]] = []):
        columns = [
            Column('link_hash', String(32), primary_key=True),
            Column('created_ts', DateTime, default=datetime.utcnow),
            Column('record_source', String(50))
        ]
        for hub_name in hub_names:
            columns.append(Column(f'{hub_name}_hash', String(32), ForeignKey(f'{hub_name}.hub_hash')))
        for col in additional_columns:
            columns.append(Column(col['name'], col['type']))
        self.db_handler.create_table(link_name, columns)