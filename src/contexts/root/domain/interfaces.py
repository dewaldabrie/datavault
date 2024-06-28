from typing import Protocol, List, Dict, Any
from sqlalchemy import Table
from src.contexts.data_landing.domain.models import DataSchema
from src.contexts.data_vault_table_creation.domain.models import ColumnSchema

class FileReader(Protocol):
    def read_schema(self, path: str) -> List[Dict[str, Any]]:
        ...

    def read_data(self, path: str) -> List[Dict[str, Any]]:
        ...

class DatabaseHandler(Protocol):
    def create_table(self, table_name: str, schema: List[ColumnSchema]) -> Table:
        ...

    def insert_data(self, table_name: str, data: List[Dict[str, Any]], exists_ok: bool = False):
        ...