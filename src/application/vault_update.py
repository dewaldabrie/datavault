from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault_table_creation.domain.models import HubSchema, SatelliteSchema, LinkSchema, ColumnSchema
from src.contexts.data_vault_data_insertion.domain.models import HubData, SatelliteData, LinkData
from datetime import datetime
from src.application.config import DB_URL

# Initialize handlers
db_handler = SQLAlchemyHandler(database_url=DB_URL)

# Dependency injection
hub_handler = HubHandler(db_handler)
satellite_handler = SatelliteHandler(db_handler)
link_handler = LinkHandler(db_handler)

# Create tables
hub_schema = HubSchema(hub_name="instrument_hub")
hub_handler.create(hub_schema)

sat_schema = SatelliteSchema(sat_name="instrument_satelite", hub_name="instrument_hub")
satellite_handler.create(sat_schema)

link_schema = LinkSchema(
    link_name="instrument_same_as_link",
    hub_names=["instrument_hub"],
    additional_columns=[ColumnSchema(name="alternative_business_key", type=str, type_length=100)]
)
link_handler.create(link_schema)

link_schema = LinkSchema(
    link_name="instrument_hierarchical_link",
    hub_names=["parent_hub", "child_hub"],
    additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=100)]
)
link_handler.create(link_schema)

# Insert data
hub_data = [
    HubData(business_key="key1", created_ts=datetime.utcnow(), record_source="source1"),
    # ... more data ...
]
hub_handler.populate("instrument_hub", hub_data)

sat_data = [
    SatelliteData(hub_hash="hash1", created_ts=datetime.utcnow(), record_source="source1", attributes={"attr1": "value1"}),
    # ... more data ...
]
satellite_handler.populate("instrument_satelite", sat_data)

link_data = [
    LinkData(link_hash="link1", created_ts=datetime.utcnow(), record_source="source1", hub_hashes=["hash1", "hash2"]),
    # ... more data ...
]
link_handler.populate("instrument_same_as_link", link_data)