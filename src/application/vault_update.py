from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_vault_table_creation.creator import DataVaultTableCreation
from src.contexts.data_vault_data_insertion.inserter import DataVaultDataInsertion
from src.contexts.data_vault_table_creation.domain.models import HubSchema, SatelliteSchema, LinkSchema, ColumnSchema
from src.contexts.data_vault_data_insertion.domain.models import HubData, SatelliteData, LinkData
from datetime import datetime
from src.application.config import DB_URL

# Initialize handlers
db_handler = SQLAlchemyHandler(database_url=DB_URL)

# Dependency injection
table_creator = DataVaultTableCreation(db_handler)
data_inserter = DataVaultDataInsertion(db_handler)

# Create tables
hub_schema = HubSchema(hub_name="instrument_hub", business_key="business_key")
table_creator.create_hub(hub_schema)

sat_schema = SatelliteSchema(sat_name="instrument_satelite", hub_name="instrument_hub")
table_creator.create_satelite(sat_schema)

link_schema = LinkSchema(
    link_name="instrument_same_as_link",
    hub_names=["instrument_hub"],
    additional_columns=[ColumnSchema(name="alternative_business_key", type=str, type_length=100)]
)
table_creator.create_link(link_schema)

link_schema = LinkSchema(
    link_name="instrument_hierarchical_link",
    hub_names=["parent_hub", "child_hub"],
    additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=100)]
)
table_creator.create_link(link_schema)

# Insert data
hub_data = [
    HubData(business_key="key1", created_ts=datetime.utcnow(), record_source="source1"),
    # ... more data ...
]
data_inserter.insert_hub_data("instrument_hub", hub_data)

sat_data = [
    SatelliteData(hub_hash="hash1", created_ts=datetime.utcnow(), record_source="source1", attributes={"attr1": "value1"}),
    # ... more data ...
]
data_inserter.insert_satelite_data("instrument_satelite", sat_data)

link_data = [
    LinkData(link_hash="link1", created_ts=datetime.utcnow(), record_source="source1", hub_hashes=["hash1", "hash2"]),
    # ... more data ...
]
data_inserter.insert_link_data("instrument_same_as_link", link_data)