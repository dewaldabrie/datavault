from typing import List
from src.contexts.root.domain.interfaces import DatabaseHandler
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
        for record in data:
            record.link_hash = hashlib.md5((record.record_source + str(record.created_ts)).encode('utf-8')).hexdigest()
        self.db_handler.insert_data(table_name, [record.dict() for record in data])

    def resize_hash_key(self, table_name: str, new_length: int):
        self.db_handler.extend_column_length(table_name, 'link_hash', new_length)