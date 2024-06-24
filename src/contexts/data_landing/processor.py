from src.infrastructure.csv_reader import CSVReader
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from typing import List, Dict
from sqlalchemy import Column


def get_columns_from_schema(schema: List[Dict[str, str]]) -> List[Column]:
    columns = []
    for field in schema:
        columns.append(Column(field['fieldname'], getattr(__import__('sqlalchemy'), field['database_type'])))
    return columns

def process_landing_data(csv_path: str, schema_path: str, db_url: str, table_name: str):
    reader = CSVReader()
    db_handler = SQLAlchemyHandler(db_url)
    
    schema = reader.read_schema(schema_path)
    columns = get_columns_from_schema(schema)
    data = reader.read_data(csv_path)
    
    db_handler.drop_table_if_exists(table_name)  # New line to drop the table if it exists
    table = db_handler.create_table(table_name, columns)
    db_handler.insert_data(table, data)