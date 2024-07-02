from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.root.domain.models import ColumnSchema
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault.domain.models import HubSchema as DomainHubSchema, SatelliteSchema as DomainSatelliteSchema, LinkSchema as DomainLinkSchema
from src.application.config import DB_URL

# Initialize handlers
db_handler = SQLAlchemyHandler(database_url=DB_URL)

# Dependency injection
hub_handler = HubHandler(db_handler)
satellite_handler = SatelliteHandler(db_handler)
link_handler = LinkHandler(db_handler)

# Create tables
hub_schema = DomainHubSchema(hub_name="instrument_hub", business_key=ColumnSchema(name="business_key", type=str, type_length=100))
hub_handler.create(hub_schema)

sat_schema = DomainSatelliteSchema(sat_name="instrument_satelite", hub_name="instrument_hub")
satellite_handler.create(sat_schema)

link_schema = DomainLinkSchema(
    link_name="instrument_same_as_link",
    hub_names=["instrument_hub"],
    additional_columns=[ColumnSchema(name="alternative_business_key", type=str, type_length=50)]
)
link_handler.create(link_schema)

link_schema = DomainLinkSchema(
    link_name="instrument_hierarchical_link",
    hub_names=["parent_hub", "child_hub"],
    additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=50)]
)
link_handler.create(link_schema)