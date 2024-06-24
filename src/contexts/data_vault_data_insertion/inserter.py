import hashlib
from typing import List, Dict, Any
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler

class DataVaultDataInsertion:
    def __init__(self, db_handler: SQLAlchemyHandler):
        self.db_handler = db_handler

    def calculate_hash(self, value: str) -> str:
        return hashlib.md5(value.encode('utf-8')).hexdigest()

    def insert_hub_data(self, hub_name: str, data: List[Dict[str, Any]]):
        for record in data:
            record['hub_hash'] = self.calculate_hash(record['business_key'])
        self.db_handler.insert_data(hub_name, data)

    def insert_satelite_data(self, sat_name: str, data: List[Dict[str, Any]]):
        for record in data:
            record['hash_diff'] = self.calculate_hash(str(record['attributes']))
        self.db_handler.insert_data(sat_name, data)

    def insert_link_data(self, link_name: str, data: List[Dict[str, Any]]):
        for record in data:
            record['link_hash'] = self.calculate_hash(record['record_source'] + str(record['created_ts']))
        self.db_handler.insert_data(link_name, data)