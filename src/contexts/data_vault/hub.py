from typing import List
from src.contexts.root.domain.interfaces import DatabaseHandler
from src.contexts.data_vault.domain.interfaces import DataVaultHandler
from src.contexts.data_vault_table_creation.domain.models import HubSchema
from src.contexts.data_vault_data_insertion.domain.models import HubData
from src.contexts.root.domain.models import ColumnSchema
import hashlib


class HubHandler(DataVaultHandler):
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler

    def create(self, schema: HubSchema, drop_existing: bool = False):
        columns = schema.columns + [ColumnSchema(name='business_key', type=str, type_length=100, unique=True, nullable=False)]
        self.db_handler.create_table(schema.hub_name, columns, drop_existing)

    def populate(self, table_name: str, data: List[HubData]):
        for record in data:
            record.hub_hash = hashlib.md5(record.business_key.encode('utf-8')).hexdigest()
        self.db_handler.insert_data(table_name, [record.dict() for record in data], exists_ok=True)

    def resize_hash_key(self, table_name: str, new_length: int):
        self.db_handler.extend_column_length(table_name, 'hub_hash', new_length)