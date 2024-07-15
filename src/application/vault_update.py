from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.root.common import HASH_FUNCTION
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault.domain.models import HubSchema as DomainHubSchema, SatelliteSchema as DomainSatelliteSchema, LinkSchema as DomainLinkSchema, ColumnSchema, HubData, SatelliteData, LinkData
from src.infrastructure.mappers import map_hub_schema_to_infra, map_satellite_schema_to_infra, map_link_schema_to_infra
from datetime import datetime
from src.application.config import DB_URL


# Initialize handlers
db_handler = SQLAlchemyHandler(database_url=DB_URL)

# Dependency injection
hub_handler = HubHandler(db_handler)
satellite_handler = SatelliteHandler(db_handler)
link_handler = LinkHandler(db_handler)

# Create tables
hub_schema = DomainHubSchema(hub_name="instrument_hub")
hub_handler.create(map_hub_schema_to_infra(hub_schema))

sat_schema = DomainSatelliteSchema(sat_name="instrument_satelite", hub_name="instrument_hub")
satellite_handler.create(map_satellite_schema_to_infra(sat_schema))

link_schema = DomainLinkSchema(
    link_name="instrument_same_as_link",
    hub_names=["instrument_hub"],
    additional_columns=[ColumnSchema(name="alternative_business_key", type=str, type_length=100)]
)
link_handler.create(map_link_schema_to_infra(link_schema))

link_schema = DomainLinkSchema(
    link_name="instrument_hierarchical_link",
    hub_names=["parent_hub", "child_hub"],
    additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=100)]
)
link_handler.create(map_link_schema_to_infra(link_schema))

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
    LinkData(link_hash="link1", created_ts=datetime.utcnow(), record_source="source1", hub_hashes={"parent_hub": "hash1", "child_hub": "hash2"}),
    # ... more data ...
]
link_handler.populate("instrument_same_as_link", link_data)

# Populate from staging
db_handler.insert_data_from_staging(
    target_table="HubData",
    staging_table="staging_hub_table",
    select_columns=['business_key', 'created_ts', 'record_source'],
    transformations={'hub_hash': (HASH_FUNCTION, ['business_key'])}
)

db_handler.insert_data_from_staging(
    target_table="SatelliteData",
    staging_table="staging_satellite_table",
    select_columns=['hub_hash', 'created_ts', 'record_source', 'attributes'],
    transformations={'hash_diff': (HASH_FUNCTION, ['attributes'])}
)

db_handler.insert_data_from_staging(
    target_table="LinkData",
    staging_table="staging_link_table",
    select_columns=['created_ts', 'record_source', 'hub_hashes'],
    transformations={'link_hash': (HASH_FUNCTION, ['record_source', 'created_ts'])}
)