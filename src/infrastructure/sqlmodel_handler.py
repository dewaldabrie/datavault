import types
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, DateTime, inspect, and_
from sqlalchemy.dialects import oracle, postgresql
from sqlalchemy import func
from sqlalchemy import select, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import NoSuchTableError
from datetime import datetime
from src.contexts.data_landing.domain.interfaces import DatabaseHandler as LandingDatabseHandler
from src.contexts.data_vault.domain.interfaces import DatabaseHandler as VaultDatabaseHandler
from src.contexts.root.domain.models import ColumnSchema
from typing import Any, Dict, List
from src.contexts.root.common import HASH_LENGTH

from sqlmodel import SQLModel, Field
from typing import List, Type
from src.contexts.data_vault.domain.models import HubSchema


def create_sqlmodel_class_old(name: str, base: Type[SQLModel], columns: List[ColumnSchema]) -> Type[SQLModel]:
    attributes = {
        col.name: Field(default=col.default, primary_key=col.primary_key, nullable=col.nullable, max_length=col.type_length, description=col.description)
        for col in columns
    }
    annotations = {col.name: col.type for col in columns}
    attributes['__annotations__'] = annotations
    return types.new_class(name, (base,), {'table': True}, lambda ns: ns.update(attributes))



class SQLModelHandler(LandingDatabseHandler, VaultDatabaseHandler):

    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.metadata = SQLModel.metadata
    
    def _create_sqlmodel_class(self, name: str, base: Type[SQLModel], columns: List[ColumnSchema], create_table=True) -> Type[SQLModel]:
        attributes = {}
        annotations = {}
        dialect = self.engine.dialect.name
        db_type_map = {
            dict: {
                # dialect: (func, lambda to transform input coln ames to func args)
                oracle.dialect.name: oracle.CLOB,  # Not sure why we don't have access to JSON type on oracle...
                postgresql.dialect.name: postgresql.JSON
            }
        }

        for col in columns:
            
            attributes[col.name] = Field(
                default=col.default, 
                primary_key=col.primary_key, 
                nullable=col.nullable, 
                max_length=col.type_length, 
                description=col.description
            )
            if col.type in db_type_map:
                field_type_lut = db_type_map.get(col.type)
                if dialect not in field_type_lut:
                    raise ValueError("Dialect-specific type mapping for {col.type} not defined for {dialect}.")
                field_type = field_type_lut[dialect]
                attributes[col.name].sa_type = field_type

            annotations[col.name] = col.type
        
        attributes['__annotations__'] = annotations
        
        return types.new_class(name, (base,), {'table': create_table}, lambda ns: ns.update(attributes))

    
    def create_table(self, table_name: str, schema: List[ColumnSchema], drop_existing: bool = False) -> None:
        
        if drop_existing:
            try:
                table = Table(table_name, SQLModel.metadata, autoload_with=self.engine)
                with self.engine.connect() as connection:
                    table.drop(connection)
                    connection.commit()
                    SQLModel.metadata.remove(table)
                    if table_name in SQLModel.metadata.tables:
                        raise ValueError(f"{table_name} still in metadata!")

            except NoSuchTableError:
                pass
            
        sql_model_class = self._create_sqlmodel_class(
            name=table_name,
            base=SQLModel,
            columns=schema
        )
        SQLModel.metadata.create_all(self.engine, tables=[sql_model_class.__table__], checkfirst=True)



    def extend_column_length(self, table_name: str, column_name: str, new_length: int) -> None:
        with self.engine.connect() as connection:
            connection.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE VARCHAR({new_length})')
            table = Table(table_name, SQLModel.metadata, autoload_with=self.engine)
            column = table.columns[column_name]
            if column.foreign_keys:
                for fk in column.foreign_keys:
                    connection.execute(f'ALTER TABLE {table_name} DROP CONSTRAINT {fk.constraint.name}')
                    connection.execute(f'ALTER TABLE {table_name} ADD CONSTRAINT {fk.constraint.name} FOREIGN KEY ({column.name}) REFERENCES {fk.column.table.name} ({fk.column.name})')

    def insert_data(self, table_name: str, data: List[Dict[str, Any]], exists_ok: bool = False):
        table_name = table_name.lower()
        if table_name in SQLModel.metadata.tables:
            table = SQLModel.metadata.tables[table_name]
        else: 
            table = self._create_sqlmodel_class(
                name=table_name,
                base=SQLModel,
                columns=[ColumnSchema(name=col, type=type(val)) for col, val in data[0].items()]
            )
        
        insert_func = pg_insert if self.engine.dialect.name == postgresql.dialect.name else insert
        with self.engine.connect() as connection:
            for record in data:
                stmt = insert_func(table).values(**record)
                if exists_ok:
                    stmt = stmt.on_conflict_do_nothing()
                connection.execute(stmt)
                connection.commit()

    def resize_hash_key(self, table_name: str, column_name: str, new_length: int):
        with self.engine.connect() as connection:
            connection.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE VARCHAR({new_length})')
            table = Table(table_name, SQLModel.metadata, autoload_with=self.engine)
            column = table.columns[column_name]
            if column.foreign_keys:
                for fk in column.foreign_keys:
                    connection.execute(f'ALTER TABLE {table_name} DROP CONSTRAINT {fk.constraint.name}')
                    connection.execute(f'ALTER TABLE {table_name} ADD CONSTRAINT {fk.constraint.name} FOREIGN KEY ({column.name}) REFERENCES {fk.column.table.name} ({fk.column.name})')
    
    def insert_data_from_staging(self, target_table: str, staging_table: str, select_columns: List[str], transformations: Dict[str, Any]):
        target_table = Table(target_table.lower(), SQLModel.metadata, autoload_with=self.engine)
        staging_table = Table(staging_table.lower(), SQLModel.metadata, autoload_with=self.engine)
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
            insert_func = pg_insert if self.engine.dialect.name == postgresql.dialect.name else insert
            insert_stmt = insert_func(target_table).from_select(column_names, select_stmt).on_conflict_do_nothing()            
            connection.execute(insert_stmt)
            connection.commit()