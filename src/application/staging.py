from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_landing.processor import process_landing_data
from src.infrastructure.csv_reader import CSVReader

# Initialize handlers
db_url = "your_database_url"
csv_path = "path_to_csv"
schema_path = "path_to_schema"
table_name = "your_table_name"

# Process landing data
process_landing_data(csv_path, schema_path, db_url, table_name)