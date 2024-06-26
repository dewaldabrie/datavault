
import datetime
import pytest
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_vault_table_creation.creator import DataVaultTableCreation
from src.contexts.data_vault_table_creation.domain.models import HubSchema, LinkSchema, SatelliteSchema, ColumnSchema
from src.contexts.data_vault_data_insertion.inserter import DataVaultDataInsertion
from src.contexts.data_vault_data_insertion.domain.models import HubData, SatelliteData, LinkData
from src.application.config import DB_URL


@pytest.fixture
def db_handler():
    return SQLAlchemyHandler(database_url=DB_URL)

@pytest.fixture
def table_creator(db_handler):
    return DataVaultTableCreation(db_handler)

@pytest.fixture
def data_inserter(db_handler):
    return DataVaultDataInsertion(db_handler)

def test_insert_hub_data(data_inserter, table_creator):
    hub_schema = HubSchema(hub_name="test_hub")
    table_creator.create_hub(hub_schema, drop_existing=True)
    hub_data = [
        HubData(business_key="key1", created_ts=datetime.datetime.now(datetime.UTC), record_source="source1"),
        # ... more data ...
    ]
    data_inserter.insert_hub_data("test_hub", hub_data)

def test_insert_satelite_data(data_inserter, table_creator):
    hub_schema = HubSchema(hub_name="test_hub")
    table_creator.create_hub(hub_schema, drop_existing=True)
    sat_schema = SatelliteSchema(sat_name="test_satelite", hub_name="test_hub")
    table_creator.create_satelite(sat_schema, drop_existing=True)
    sat_data = [
        SatelliteData(hub_hash="hash1", created_ts=datetime.datetime.now(datetime.UTC), record_source="source1", attributes={"attr1": "value1"}),
        # ... more data ...
    ]
    data_inserter.insert_satelite_data("test_satelite", sat_data)

def test_insert_link_data(data_inserter, table_creator):
    hub_schema = HubSchema(hub_name="test_hub1")
    table_creator.create_hub(hub_schema, drop_existing=True)
    hub_schema = HubSchema(hub_name="test_hub2")
    table_creator.create_hub(hub_schema, drop_existing=True)
    link_schema = LinkSchema(
        link_name="test_link",
        hub_names=["test_hub1", "test_hub2"],
        additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=50)]
    )
    table_creator.create_link(link_schema, drop_existing=True)
    link_data = [
        LinkData(created_ts=datetime.datetime.now(datetime.UTC), record_source="source1", hub_hashes=["hash1", "hash2"]),
        # ... more data ...
    ]
    data_inserter.insert_link_data("test_link", link_data)