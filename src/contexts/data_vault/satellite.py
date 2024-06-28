
from typing import List
from src.contexts.root.domain.interfaces import DatabaseHandler
from src.contexts.data_vault.domain.models import SatelliteSchema
from src.contexts.data_vault.domain.models import SatelliteData
from src.contexts.data_vault.domain.interfaces import DataVaultHandler
import hashlib


class SatelliteHandler(DataVaultHandler):
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler

    def create(self, schema: SatelliteSchema, drop_existing: bool = False):
        for col in schema.columns:
            if col.name == 'hub_hash':
                col.foreign_key = f'{schema.hub_name}.hub_hash'
        self.db_handler.create_table(schema.sat_name, schema.columns, drop_existing)

    def populate(self, table_name: str, data: List[SatelliteData]):
        for record in data:
            record.hash_diff = hashlib.md5(str(record.attributes).encode('utf-8')).hexdigest()
        self.db_handler.insert_data(table_name, [record.dict() for record in data])

    def resize_hash_key(self, table_name: str, new_length: int):
        self.db_handler.extend_column_length(table_name, 'hash_diff', new_length)