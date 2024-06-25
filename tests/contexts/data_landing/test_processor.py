import os
import pytest
from src.infrastructure.csv_reader import CSVReader
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_landing.processor import process_landing_data

@pytest.mark.parametrize("csv_path, schema_path, table_name", [
    ("tests/data/nyse_equities.csv", "tests/data/nyse_equities_schema.json", "nyse_equities"),
    ("tests/data/nyse_options.csv", "tests/data/nyse_options_schema.json", "nyse_options"),
    ("tests/data/lse_equities.csv", "tests/data/lse_equities_schema.json", "lse_equities"),
    ("tests/data/lse_options.csv", "tests/data/lse_options_schema.json", "lse_options")
])
def test_landing_data(csv_path, schema_path, table_name):
    db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD')}@localhost/postgres"
    process_landing_data(csv_path, schema_path, db_url, table_name)
    # Additional assertions to verify data integrity in the database can be added here