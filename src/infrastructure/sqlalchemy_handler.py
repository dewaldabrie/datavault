from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, DateTime, inspect, and_
from sqlalchemy.dialects.postgresql import JSON
from datetime import datetime
from src.domain.interfaces import DatabaseHandler
from src.contexts.root.domain.models import ColumnSchema
from typing import Any, Dict, List

class SQLAlchemyHandler(DatabaseHandler):
    _TYPE_MAP = {
        str: String,
        int: Integer,
        datetime: DateTime,
        dict: JSON # detect Postgres, else fall back to string
    }
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
        self.metadata = MetaData()

    def _get_column_from_column_schema(self, column_schema: ColumnSchema) -> Column:
        db_type = self._TYPE_MAP.get(column_schema.type, String)
        if column_schema.type_length:
            db_type = db_type(column_schema.type_length)
        col = Column(
            column_schema.name, 
            db_type,
            default=column_schema.default,
            primary_key=column_schema.primary_key,
            nullable=column_schema.nullable,
            unique=column_schema.unique,
            comment=column_schema.description
        )
        return col

    def drop_table_if_exists(self, name: str):
        if inspect(self.engine).has_table(name):
            table = Table(name, self.metadata, autoload_with=self.engine)
            table.drop(self.engine)
    
    def create_table(self, table_name: str, columns: List[ColumnSchema], drop_existing: bool = False):
        # drop table if exists
        if drop_existing and inspect(self.engine).has_table(table_name):
            self.drop_table_if_exists(table_name)
        # create table if not exists
        if not inspect(self.engine).has_table(table_name):
            table = Table(table_name, self.metadata, *[self._get_column_from_column_schema(column) for column in columns], extend_existing=True)
            table.create(self.engine)
            return table

    def insert_data(self, table_name: str, data: List[Dict[str, Any]], exists_ok: bool = False):
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as connection:
            # insert if not exists
            if not exists_ok and inspect(self.engine).has_table(table_name):
                # get primary key columns (potentially compound) names
                primary_key_columns = [col.name for col in table.primary_key.columns]
                for record in data:
                    # this assumes the table has a primary key defined
                    if not connection.execute(table.select().where(and_(*[table.primary_key == record[col] for col in primary_key_columns]))).fetchone():
                        connection.execute(table.insert(), record)
            else:
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