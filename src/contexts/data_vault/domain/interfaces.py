from typing import List, Protocol, Dict, Any
from src.contexts.root.domain.models import ColumnSchema
from src.contexts.data_vault.domain.models import HubSchema
from src.contexts.data_vault.domain.models import HubData


class DataVaultHandler(Protocol):
    def create(self, schema: HubSchema, drop_existing: bool = False):
        ...

    def populate(self, table_name: str, data: List[HubData]):
        ...

    def resize_hash_key(self, table_name: str, new_length: int):
        ...
    
    def insert_data_from_staging(self, target_table: str, staging_table: str, select_columns: List[str], transformations: Dict[str, Any]):
        ...


class DatabaseHandler(Protocol):
    def create_table(self, table_name: str, schema: List[ColumnSchema], drop_existing: bool = False) -> None:
        ...

    def insert_data(self, table_name: str, data: List[Dict[str, Any]], exists_ok: bool = False):
        ...

    def resize_hash_key(self, table_name: str, column_name: str, new_length: int):
        ...
    
    def insert_data_from_staging(self, target_table: str, staging_table: str, select_columns: List[str], transformations: Dict[str, Any]):
        ...