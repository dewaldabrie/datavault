# src/application/staging.py
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.infrastructure.csv_reader import CSVReader
from src.application.config import DB_URL
from src.infrastructure.mappers import map_data_schema_to_column_schemas
from src.contexts.data_landing.domain.models import DataSchema, FieldSchema

# Initialize handlers
db_handler = SQLAlchemyHandler(database_url=DB_URL)
reader = CSVReader()

# Read schema and data
schema_dict = reader.read_schema("path_to_schema")
data_schema = DataSchema(fields=[FieldSchema(**field) for field in schema_dict])
columns = map_data_schema_to_column_schemas(data_schema)

# Create table and insert data
db_handler.create_table("your_table_name", columns)
data_dicts = reader.read_data("path_to_csv")
db_handler.insert_data("your_table_name", data_dicts)