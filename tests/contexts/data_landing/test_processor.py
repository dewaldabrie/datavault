import os
import pytest
from dotenv import load_dotenv, find_dotenv

from src.infrastructure.csv_reader import CSVReader
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_landing.processor import process_landing_data
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
    process_landing_data(csv_path, schema_path, table_name, reader, db_handler, primary_key_field='ISIN')
    # Additional assertions to verify data integrity in the database can be added here