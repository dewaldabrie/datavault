
import pytest
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_vault_data_insertion.inserter import DataVaultDataInsertion
from src.contexts.data_vault_data_insertion.domain.models import HubData, SatelliteData, LinkData
from datetime import datetime

@pytest.fixture
def db_handler():
    return SQLAlchemyHandler(database_url="your_database_url")

@pytest.fixture
def data_inserter(db_handler):
    return DataVaultDataInsertion(db_handler)

def test_insert_hub_data(data_inserter):
    hub_data = [
        HubData(business_key="key1", created_ts=datetime.utcnow(), record_source="source1"),
        # ... more data ...
    ]
    data_inserter.insert_hub_data("test_hub", hub_data)
    # Add assertions to verify data insertion

def test_insert_satelite_data(data_inserter):
    sat_data = [
        SatelliteData(hub_hash="hash1", created_ts=datetime.utcnow(), record_source="source1", attributes={"attr1": "value1"}),
        # ... more data ...
    ]
    data_inserter.insert_satelite_data("test_satelite", sat_data)
    # Add assertions to verify data insertion

def test_insert_link_data(data_inserter):
    link_data = [
        LinkData(link_hash="link1", created_ts=datetime.utcnow(), record_source="source1", hub_hashes=["hash1", "hash2"]),
        # ... more data ...
    ]
    data_inserter.insert_link_data("test_link", link_data)
    # Add assertions to verify data insertion