import pytest
from src.infrastructure.sqlmodel_handler import SQLModelHandler
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault.domain.models import HubSchema as DomainHubSchema, SatelliteSchema as DomainSatelliteSchema, LinkSchema as DomainLinkSchema, ColumnSchema
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

def test_create_hub(hub_handler):
    hub_schema = DomainHubSchema(hub_name="test_hub", business_key=ColumnSchema(name="business_key", type=str, type_length=100, unique=True, nullable=False))
    hub_handler.create(hub_schema)
    # Add assertions to verify table creation

def test_create_satelite(satellite_handler):
    sat_schema = DomainSatelliteSchema(sat_name="test_satelite", hub_name="test_hub")
    satellite_handler.create(sat_schema)
    # Add assertions to verify table creation

def test_create_link(link_handler):
    link_schema = DomainLinkSchema(
        link_name="test_link",
        hub_names=["test_hub1", "test_hub2"],
        additional_columns=[ColumnSchema(name="additional_col", type=str, type_length=50)]
    )
    link_handler.create(link_schema)
    # Add assertions to verify table creation