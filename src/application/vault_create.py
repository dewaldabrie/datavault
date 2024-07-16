from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.contexts.root.domain.models import ColumnSchema
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault.domain.models import HubSchema as HubSchema, SatelliteSchema as SatelliteSchema, LinkSchema as LinkSchema
from src.application.config import DB_URL

# Initialize handlers
db_handler = SQLAlchemyHandler(database_url=DB_URL)

# Dependency injection
hub_handler = HubHandler(db_handler)
satellite_handler = SatelliteHandler(db_handler)
link_handler = LinkHandler(db_handler)

# Create tables
hub_schema = HubSchema(hub_name="hub_ins", business_key=ColumnSchema(name="primary_symbol", type=str, type_length=100, unique=False, nullable=False))
hub_handler.create(hub_schema, drop_existing=True)

sat_schema = SatelliteSchema(sat_name="sat_dsp_opt", hub_name="hub_ins")
satellite_handler.create(sat_schema, drop_existing=True)

sat_schema = SatelliteSchema(sat_name="sat_dsp_eq", hub_name="hub_ins")
satellite_handler.create(sat_schema, drop_existing=True)

sat_schema = SatelliteSchema(sat_name="sat_bbgbo_eq", hub_name="hub_ins")
satellite_handler.create(sat_schema, drop_existing=True)

hub_schema = HubSchema(hub_name="hub_ins_symbol", business_key=ColumnSchema(name="symbol", type=str, type_length=100, unique=False, nullable=False))
hub_handler.create(hub_schema, drop_existing=True)

link_schema = LinkSchema(
    link_name="link_ins_symbol",
    hub_names=["hub_ins", "hub_ins_symbol"],
)
link_handler.create(link_schema, drop_existing=True)

hub_schema = HubSchema(hub_name="hub_sym_fam", business_key=ColumnSchema(name="family_name", type=str, type_length=100, unique=True, nullable=False))
hub_handler.create(hub_schema, drop_existing=True)

link_schema = LinkSchema(
    link_name="link_symbol_family",
    hub_names=["hub_ins_symbol", "hub_sym_fam"],
)
link_handler.create(link_schema, drop_existing=True)

link_schema = LinkSchema(
    link_name="link_ins_hierarchy",
    hub_names=["hub_ins", "hub_ins"],
    additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=50)]
)
link_handler.create(link_schema, drop_existing=True)

link_schema = LinkSchema(
    link_name="link_ins_sameas",
    hub_names=["hub_ins", "hub_ins"],
    additional_columns=[ColumnSchema(name="relationship_type", type=str, type_length=50)]
)
link_handler.create(link_schema, drop_existing=True)