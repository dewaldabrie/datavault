import datetime
import pytest
from src.infrastructure.sqlmodel_handler import SQLModelHandler
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault.domain.models import HubSchema as DomainHubSchema, LinkSchema as DomainLinkSchema, SatelliteSchema as DomainSatelliteSchema, ColumnSchema, HubData, SatelliteData, LinkData
from src.application.config import DB_URL

@pytest.fixture
def db_handler():
    return SQLModelHandler(database_url=DB_URL)

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
    hub_schema = DomainHubSchema(hub_name="test_hub")
    hub_handler.create(hub_schema, drop_existing=True)
    hub_data = [
        HubData(business_key="key1", created_ts=datetime.datetime.now(datetime.UTC), record_source="source1"),
        # ... more data ...
    ]
    hub_handler.populate("test_hub", hub_data)

def test_insert_satelite_data(satellite_handler, hub_handler):
    hub_schema = DomainHubSchema(hub_name="test_hub")
    hub_handler.create(hub_schema, drop_existing=True)
    sat_schema = DomainSatelliteSchema(sat_name="test_satelite", hub_name="test_hub")
    satellite_handler.create(sat_schema, drop_existing=True)
    sat_data = [
        SatelliteData(hub_hash="hash1", created_ts=datetime.datetime.now(datetime.UTC), record_source="source1", attributes={"attr1": "value1"}),
        # ... more data ...
    ]
    satellite_handler.populate("test_satelite", sat_data)

def test_insert_link_data(link_handler, hub_handler):
    hub_schema = DomainHubSchema(hub_name="test_hub1")
    hub_handler.create(hub_schema, drop_existing=True)
    hub_schema = DomainHubSchema(hub_name="test_hub2")
    hub_handler.create(hub_schema, drop_existing=True)
    link_schema = DomainLinkSchema(
        link_name="test_link",
        hub_names=["test_hub1", "test_hub2"],
        additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=50)]
    )
    link_handler.create(link_schema, drop_existing=True)
    link_data = [
        LinkData(created_ts=datetime.datetime.now(datetime.UTC), record_source="source1", hub_hashes={"test_hub1": "hash1", "test_hub2": "hash2"}),
        # ... more data ...
    ]
    link_handler.populate("test_link", link_data)

def test_insert_data_from_staging(db_handler):
    db_handler.create_table("staging_hub_table", [
        ColumnSchema(name="business_key", type=str, type_length=100, primary_key=True),
        ColumnSchema(name="created_ts", type=datetime.datetime),
        ColumnSchema(name="record_source", type=str, type_length=500)
    ], drop_existing=True)

    db_handler.insert_data("staging_hub_table", [
        {"business_key": "key1", "created_ts": datetime.datetime.now(datetime.UTC), "record_source": "source1"},
        # ... more data ...
    ])

    db_handler.create_table("HubData", [
        ColumnSchema(name="business_key", type=str, type_length=100),
        ColumnSchema(name="created_ts", type=datetime.datetime),
        ColumnSchema(name="record_source", type=str, type_length=500),
        ColumnSchema(name="hub_hash", type=str, type_length=32, primary_key=True)
    ], drop_existing=True)

    db_handler.insert_data_from_staging(
        target_table="HubData",
        staging_table="staging_hub_table",
        select_columns=['business_key', 'created_ts', 'record_source'],
        transformations={'hub_hash': ('md5', ['business_key'])}
    )

    # Add assertions to verify data insertion
    from sqlalchemy import text
    with db_handler.engine.connect() as connection:
        result = connection.execute(text('SELECT * FROM hubdata')).fetchall()
    assert len(result) > 0
    hub_hash_idx = -1
    assert result[0][hub_hash_idx] is not None