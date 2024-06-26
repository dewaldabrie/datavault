import pytest
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_vault_table_creation.creator import DataVaultTableCreation
from src.contexts.data_vault_table_creation.domain.models import HubSchema, SatelliteSchema, LinkSchema
from src.contexts.root.domain.models import ColumnSchema


import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD')}@localhost/postgres"

@pytest.fixture
def db_handler():
    return SQLAlchemyHandler(database_url=db_url)

@pytest.fixture
def table_creator(db_handler):
    return DataVaultTableCreation(db_handler)

def test_create_hub(table_creator):
    hub_schema = HubSchema(hub_name="test_hub", business_key=ColumnSchema(name="business_key", type=str, type_length=100, unique=True, nullable=False))
    table_creator.create_hub(hub_schema)
    # Add assertions to verify table creation

def test_create_satelite(table_creator):
    sat_schema = SatelliteSchema(sat_name="test_satelite", hub_name="test_hub")
    table_creator.create_satelite(sat_schema)
    # Add assertions to verify table creation

def test_create_link(table_creator):
    link_schema = LinkSchema(
        link_name="test_link",
        hub_names=["test_hub1", "test_hub2"],
        additional_columns=[ColumnSchema(name="additional_col", type=str, type_length=50)]
    )
    table_creator.create_link(link_schema)
    # Add assertions to verify table creation