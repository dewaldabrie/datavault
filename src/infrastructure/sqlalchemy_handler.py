from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, DateTime, inspect, and_
from sqlalchemy.dialects import oracle, postgresql
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy import func
from sqlalchemy import select, insert
from datetime import datetime
from src.contexts.data_landing.domain.interfaces import DatabaseHandler as LandingDatabseHandler
from src.contexts.data_vault.domain.interfaces import DatabaseHandler as VaultDatabaseHandler
from src.contexts.root.domain.models import ColumnSchema
from typing import Any, Dict, List
from src.contexts.root.common import HASH_LENGTH

class SQLAlchemyHandler(LandingDatabseHandler, VaultDatabaseHandler):
    _TYPE_MAP = {
        str: String,
        int: Integer,
        datetime: DateTime,
        dict: JSON # detect Postgres, else fall back to string
    }
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.metadata = MetaData()

    def _get_infra_column_from_domain_column_schema(self, column_schema: ColumnSchema) -> Column:
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
            with self.engine.connect() as connection:
                table.drop(connection)
                connection.commit()
    
    def create_table(self, table_name: str, columns: List[ColumnSchema], drop_existing: bool = False):
        # drop table if exists
            if drop_existing:
                self.drop_table_if_exists(table_name)
            # create table if not exists
            if not inspect(self.engine).has_table(table_name):
                table = Table(table_name, self.metadata, *[self._get_infra_column_from_domain_column_schema(column) for column in columns], extend_existing=True)
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
            
            connection.commit()


    def extend_column_length(self, table_name: str, column_name: str, new_length: int):
        with self.engine.connect() as connection:
            connection.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE VARCHAR({new_length})')
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            column = table.columns[column_name]
            if column.foreign_keys:
                for fk in column.foreign_keys:
                    connection.execute(f'ALTER TABLE {table_name} DROP CONSTRAINT {fk.constraint.name}')
                    connection.execute(f'ALTER TABLE {table_name} ADD CONSTRAINT {fk.constraint.name} FOREIGN KEY ({column.name}) REFERENCES {fk.column.table.name} ({fk.column.name})')

    
    def insert_data_from_staging(self, target_table: str, staging_table: str, select_columns: List[str], transformations: Dict[str, Any]):
        target_table = Table(target_table, self.metadata, autoload_with=self.engine)
        staging_table = Table(staging_table, self.metadata, autoload_with=self.engine)
        # for different db dialects, map the hash function to a db function
        dialect = self.engine.dialect.name
        from sqlalchemy import func
        db_func_map = {
            'md5': {
                # dialect: (func, lambda to transform input coln ames to func args)
                oracle.dialect.name: (func.STANDARD_HASH, lambda input_col_names: [func.cast(func.concat(*[staging_table.c[col_name] for col_name in input_col_names]), oracle.CHAR(HASH_LENGTH))]),
                postgresql.dialect.name: (func.md5, lambda input_col_names: [func.concat(*[staging_table.c[col_name] for col_name in input_col_names])])
            }
        }
        with self.engine.connect() as connection:
            computed_cols = []
            for col_alias, trans in transformations.items():
                trans_func_name, input_col_names = trans
                dialect_func_map = db_func_map.get(trans_func_name)
                if dialect_func_map:
                    tx_func, args_lambda = dialect_func_map.get(dialect)
                    if func:
                        args = args_lambda(input_col_names)
                        computed_cols.append(tx_func(*args).label(col_alias))
            
            select_cols = [staging_table.c[col] for col in select_columns]
            select_stmt = select(*select_cols, *computed_cols).select_from(staging_table)
            column_names = [column.name for column in staging_table.c] + [col_alias for col_alias in transformations.keys()]
            insert_stmt = insert(target_table).from_select(column_names, select_stmt)
            connection.execute(insert_stmt)
            connection.commit()