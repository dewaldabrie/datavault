from typing import Protocol, List, Dict, Any
from sqlalchemy import Table
from src.contexts.data_landing.domain.models import DataSchema

class FileReader(Protocol):
    def read_schema(self, path: str) -> List[Dict[str, Any]]:
        ...

    def read_data(self, path: str) -> List[Dict[str, Any]]:
        ...

class DatabaseHandler(Protocol):
    def create_table(self, name: str, schema: List[Dict[str, Any]]) -> Table:
        ...

    def insert_data(self, table: Table, data: List[Dict[str, Any]]):
        ...