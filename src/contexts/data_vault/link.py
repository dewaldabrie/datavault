import dataclasses
from typing import List, Dict, Any
from src.contexts.data_vault.domain.interfaces import DatabaseHandler
from src.contexts.data_vault.domain.interfaces import DataVaultHandler
from src.contexts.data_vault.domain.models import LinkSchema, LinkData
from src.contexts.root.domain.models import ColumnSchema
from src.contexts.root.common import HASH_LENGTH
import hashlib


class LinkHandler(DataVaultHandler):
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler

    def create(self, schema: LinkSchema, drop_existing: bool = False):
        for hub_name in schema.hub_names:
            schema.columns.append(ColumnSchema(name=f'{hub_name}_hash', type=str, type_length=HASH_LENGTH, foreign_key=f'{hub_name}.hub_hash'))
        schema.columns.extend(schema.additional_columns)
        self.db_handler.create_table(schema.link_name, schema.columns, drop_existing)

    def populate(self, table_name: str, data: List[LinkData]):
        record_dicts = []
        for record in data:
            record.link_hash = hashlib.md5((record.record_source + str(record.created_ts)).encode('utf-8')).hexdigest()
            d = dataclasses.asdict(record)
            # translate LinkData to LinkSchema
            hub_hashes = d.pop('hub_hashes')
            for hub_name, hub_hash_key in hub_hashes.items():
                d[f'{hub_name}_hash'] = hub_hash_key
            record_dicts.append(d)
        self.db_handler.insert_data(table_name, record_dicts)

    def resize_hash_key(self, table_name: str, new_length: int):
        self.db_handler.extend_column_length(table_name, 'link_hash', new_length)

    def insert_data_from_staging(self, target_table: str, staging_table: str, select_columns: List[str], transformations: Dict[str, Any]):
        self.db_handler.insert_data_from_staging(target_table, staging_table, select_columns, transformations)