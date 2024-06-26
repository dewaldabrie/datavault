from src.domain.interfaces import DatabaseHandler
from src.contexts.data_vault_table_creation.domain.models import HubSchema, SatelliteSchema, LinkSchema
from src.contexts.root.domain.models import ColumnSchema
from src.contexts.root.common import HASH_LENGTH


class DataVaultTableCreation:
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler

    def create_hub(self, hub_schema: HubSchema, drop_existing: bool = False):
        columns = hub_schema.columns + [ColumnSchema(name='business_key', type=str, type_length=100, unique=True, nullable=False)]
        self.db_handler.create_table(hub_schema.hub_name, columns, drop_existing)

    def create_satelite(self, sat_schema: SatelliteSchema, drop_existing: bool = False):
        for col in sat_schema.columns:
            if col.name == 'hub_hash':
                col.foreign_key = f'{sat_schema.hub_name}.hub_hash'
        self.db_handler.create_table(sat_schema.sat_name, sat_schema.columns, drop_existing)

    def create_link(self, link_schema: LinkSchema, drop_existing: bool = False):
        for hub_name in link_schema.hub_names:
            link_schema.columns.append(ColumnSchema(name=f'{hub_name}_hash', type=str, type_length=HASH_LENGTH, foreign_key=f'{hub_name}.hub_hash'))
        link_schema.columns.extend(link_schema.additional_columns)
        self.db_handler.create_table(link_schema.link_name, link_schema.columns, drop_existing)