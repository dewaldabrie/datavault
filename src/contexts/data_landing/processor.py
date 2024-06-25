from src.infrastructure.csv_reader import CSVReader
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_landing.domain.models import DataSchema, create_record_model
from typing import List, Dict
from sqlalchemy import Column, String, Integer

def get_columns_from_schema(schema: DataSchema) -> List[Column]:
    columns = []
    for field in schema.fields:
        col_type = getattr(__import__('sqlalchemy'), field.db_type)
        if field.db_col_length:
            col_type = col_type(field.db_col_length)
        columns.append(Column(field.fieldname, col_type))
    return columns

def process_landing_data(csv_path: str, schema_path: str, db_url: str, table_name: str):
    reader = CSVReader()
    db_handler = SQLAlchemyHandler(db_url)
    
    schema_dict = reader.read_schema(schema_path)
    schema = DataSchema(fields=[FieldSchema(**field) for field in schema_dict])
    columns = get_columns_from_schema(schema)
    data_dicts = reader.read_data(csv_path)
    
    RecordModel = create_record_model(schema)
    data = [RecordModel(**record).dict() for record in data_dicts]
    
    db_handler.drop_table_if_exists(table_name)
    table = db_handler.create_table(table_name, columns)
    db_handler.insert_data(table, data)