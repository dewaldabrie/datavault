from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_landing.processor import process_landing_data
from src.infrastructure.csv_reader import CSVReader
from src.application.config import DB_URL

# Initialize handlers
db_url = "your_database_url"
csv_path = "path_to_csv"
schema_path = "path_to_schema"
table_name = "your_table_name"
primary_key_field = "ISIN"

# Process landing data
reader = CSVReader()
db_handler = SQLAlchemyHandler(DB_URL)
process_landing_data(csv_path, schema_path, table_name, reader, db_handler, primary_key_field)