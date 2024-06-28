import datetime
import pytest
from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault_table_creation.domain.models import HubSchema, LinkSchema, SatelliteSchema, ColumnSchema
from src.contexts.data_vault_data_insertion.domain.models import HubData, SatelliteData, LinkData
from src.application.config import DB_URL

@pytest.fixture
def db_handler():
    return SQLAlchemyHandler(database_url=DB_URL)

@pytest.fixture
def hub_handler(db_handler):
    return HubHandler(db_handler)

@pytest.fixture
def satellite_handler(db_handler):
    return SatelliteHandler(db_handler)

@pytest.fixture
def link_handler(db_handler):
    return LinkHandler(db_handler)

def test_insert_hub_data(hub_handler):
    hub_schema = HubSchema(hub_name="test_hub")
    hub_handler.create(hub_schema, drop_existing=True)
    hub_data = [
        HubData(business_key="key1", created_ts=datetime.datetime.now(datetime.UTC), record_source="source1"),
        # ... more data ...
    ]
    hub_handler.populate("test_hub", hub_data)

def test_insert_satelite_data(satellite_handler, hub_handler):
    hub_schema = HubSchema(hub_name="test_hub")
    hub_handler.create(hub_schema, drop_existing=True)
    sat_schema = SatelliteSchema(sat_name="test_satelite", hub_name="test_hub")
    satellite_handler.create(sat_schema, drop_existing=True)
    sat_data = [
        SatelliteData(hub_hash="hash1", created_ts=datetime.datetime.now(datetime.UTC), record_source="source1", attributes={"attr1": "value1"}),
        # ... more data ...
    ]
    satellite_handler.populate("test_satelite", sat_data)

def test_insert_link_data(link_handler, hub_handler):
    hub_schema = HubSchema(hub_name="test_hub1")
    hub_handler.create(hub_schema, drop_existing=True)
    hub_schema = HubSchema(hub_name="test_hub2")
    hub_handler.create(hub_schema, drop_existing=True)
    link_schema = LinkSchema(
        link_name="test_link",
        hub_names=["test_hub1", "test_hub2"],
        additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=50)]
    )
    link_handler.create(link_schema, drop_existing=True)
    link_data = [
        LinkData(created_ts=datetime.datetime.now(datetime.UTC), record_source="source1", hub_hashes=["hash1", "hash2"]),
        # ... more data ...
    ]
    link_handler.populate("test_link", link_data)