import dataclasses
from typing import List, Dict, Any
from src.contexts.data_vault.domain.interfaces import DatabaseHandler
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
        self.db_handler.insert_data(table_name, [dataclasses.asdict(record) for record in data])

    def resize_hash_key(self, table_name: str, new_length: int):
        self.db_handler.extend_column_length(table_name, 'hash_diff', new_length)

    def insert_data_from_staging(self, target_table: str, staging_table: str, select_columns: List[str], transformations: Dict[str, Any]):
        self.db_handler.insert_data_from_staging(target_table, staging_table, select_columns, transformations)