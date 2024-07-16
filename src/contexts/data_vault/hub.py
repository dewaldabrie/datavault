import dataclasses
from typing import List, Dict, Any
from src.contexts.data_vault.domain.interfaces import DatabaseHandler, DataVaultHandler
from src.contexts.data_vault.domain.models import HubSchema, HubData
from src.contexts.root.domain.models import ColumnSchema
from src.contexts.root.common import calculate_hash_key


class HubHandler(DataVaultHandler):
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler

    def create(self, schema: HubSchema, drop_existing: bool = False):
        columns = schema.columns + [schema.business_key]
        self.db_handler.create_table(schema.hub_name, columns, drop_existing)

    def populate(self, table_name: str, data: List[HubData]):
        self.db_handler.insert_data(table_name, [dataclasses.asdict(record) for record in data], exists_ok=True)

    def resize_hash_key(self, table_name: str, new_length: int):
        self.db_handler.extend_column_length(table_name, 'hash_key', new_length)

    def insert_data_from_staging(self, target_table: str, staging_table: str, select_columns: List[str], transformations: Dict[str, Any]):
        self.db_handler.insert_data_from_staging(target_table, staging_table, select_columns, transformations)