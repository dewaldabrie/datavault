from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, DateTime, inspect
from src.domain.interfaces import DatabaseHandler
from datetime import datetime
from typing import Any, Dict, List

class SQLAlchemyHandler(DatabaseHandler):
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
        self.metadata = MetaData()

    def drop_table_if_exists(self, name: str):
        if inspect(self.engine).has_table(name):
            table = Table(name, self.metadata, autoload_with=self.engine)
            table.drop(self.engine)
    
    def create_table(self, table_name: str, columns: List[Column]):
        table = Table(table_name, self.metadata, *columns, extend_existing=True)
        table.create(self.engine)
        return table

    def insert_data(self, table_name: str, data: List[Dict[str, Any]]):
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as connection:
            connection.execute(table.insert(), data)

    def extend_column_length(self, table_name: str, column_name: str, new_length: int):
        with self.engine.connect() as connection:
            connection.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE VARCHAR({new_length})')
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            column = table.columns[column_name]
            if column.foreign_keys:
                for fk in column.foreign_keys:
                    connection.execute(f'ALTER TABLE {table_name} DROP CONSTRAINT {fk.constraint.name}')
                    connection.execute(f'ALTER TABLE {table_name} ADD CONSTRAINT {fk.constraint.name} FOREIGN KEY ({column.name}) REFERENCES {fk.column.table.name} ({fk.column.name})')