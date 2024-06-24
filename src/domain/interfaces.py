from typing import Protocol, List, Dict, Any
from sqlalchemy import Table

class FileReader(Protocol):
    def read_schema(self, path: str) -> List[Dict[str, Any]]:
        pass

    def read_data(self, path: str) -> List[Dict[str, Any]]:
        pass

class DatabaseHandler(Protocol):
    def create_table(self, name: str, schema: List[Dict[str, Any]]) -> Table:
        pass

    def insert_data(self, table: Table, data: List[Dict[str, Any]]):
        pass