from src.domain.interfaces import FileReader, DatabaseHandler
from src.contexts.data_landing.domain.models import DataSchema, create_record_model
from src.contexts.root.domain.models import FieldSchema, ColumnSchema
from typing import List


def get_column_schema_from_field_schema(schema: FieldSchema, primary_key_field:str = None) -> List[ColumnSchema]:
    return ColumnSchema(
        name=schema.fieldname,
        type=schema.python_type,
        type_length=schema.db_col_length,
        description=schema.description,
        primary_key=schema.fieldname == primary_key_field
    )

def process_landing_data(csv_path: str, schema_path: str, table_name: str, reader: FileReader, db_handler: DatabaseHandler, primary_key_field: str = None):
    schema_dict = reader.read_schema(schema_path)
    schema = DataSchema(fields=[FieldSchema(**field) for field in schema_dict])
    columns = list(map(lambda field: get_column_schema_from_field_schema(field, primary_key_field=primary_key_field), schema.fields))

    data_dicts = reader.read_data(csv_path)
    
    RecordModel = create_record_model(schema)
    data = [RecordModel(**record).dict() for record in data_dicts]
    
    db_handler.drop_table_if_exists(table_name)
    table = db_handler.create_table(table_name, columns)
    db_handler.insert_data(table_name, data)