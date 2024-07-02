import os
import pytest
from dotenv import load_dotenv, find_dotenv

from src.infrastructure.csv_reader import CSVReader
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_landing.domain.models import DataSchema, FieldSchema
from src.infrastructure.mappers import map_data_schema_to_column_schemas
from src.application.config import DB_URL

@pytest.mark.parametrize("csv_path, schema_path, table_name", [
    ("tests/data/nyse_equities.csv", "tests/data/nyse_equities_schema.json", "nyse_equities"),
    ("tests/data/nyse_options.csv", "tests/data/nyse_options_schema.json", "nyse_options"),
    ("tests/data/lse_equities.csv", "tests/data/lse_equities_schema.json", "lse_equities"),
    ("tests/data/lse_options.csv", "tests/data/lse_options_schema.json", "lse_options")
])
def test_landing_data(csv_path, schema_path, table_name):
    reader = CSVReader()
    db_handler = SQLAlchemyHandler(DB_URL)

    db_handler = SQLAlchemyHandler(database_url=DB_URL)
    reader = CSVReader()

    # Read schema and data
    schema_dict = reader.read_schema(schema_path)
    data_schema = DataSchema(fields=[FieldSchema(**field) for field in schema_dict])
    columns = map_data_schema_to_column_schemas(data_schema)

    # Create table and insert data
    db_handler.create_table(table_name, columns, drop_existing=True)
    data_dicts = reader.read_data(csv_path)
    db_handler.insert_data(table_name, data_dicts)