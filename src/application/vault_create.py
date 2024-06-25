from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.data_vault_table_creation.creator import DataVaultTableCreation
from src.contexts.data_vault_table_creation.domain.models import HubSchema, SatelliteSchema, LinkSchema, ColumnSchema
from datetime import datetime
from sqlalchemy import String

# Initialize handlers
db_handler = SQLAlchemyHandler(database_url="your_database_url")

# Dependency injection
table_creator = DataVaultTableCreation(db_handler)

# Create tables
hub_schema = HubSchema(hub_name="instrument_hub", business_key="business_key")
table_creator.create_hub(hub_schema)

sat_schema = SatelliteSchema(sat_name="instrument_satelite", hub_name="instrument_hub")
table_creator.create_satelite(sat_schema)

link_schema = LinkSchema(
    link_name="instrument_same_as_link",
    hub_names=["instrument_hub"],
    additional_columns=[ColumnSchema(name="alternative_business_key", type=String(50))]
)
table_creator.create_link(link_schema)

link_schema = LinkSchema(
    link_name="instrument_hierarchical_link",
    hub_names=["parent_hub", "child_hub"],
    additional_columns=[ColumnSchema(name="relationship_type", type=String(50))]
)
table_creator.create_link(link_schema)