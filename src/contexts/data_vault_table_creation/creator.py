from sqlalchemy import Column, String, DateTime, JSON, ForeignKey
from datetime import datetime
from typing import List, Dict, Any
from src.domain.interfaces import DatabaseHandler
from src.contexts.data_vault_table_creation.domain.models import HubSchema, SatelliteSchema, LinkSchema, ColumnSchema

class DataVaultTableCreation:
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler

    def create_hub(self, hub_schema: HubSchema):
        columns = hub_schema.columns + [Column(hub_schema.business_key, String(50))]
        self.db_handler.create_table(hub_schema.hub_name, columns)

    def create_satelite(self, sat_schema: SatelliteSchema):
        for col in sat_schema.columns:
            if col.name == 'hub_hash':
                col.foreign_key = f'{sat_schema.hub_name}.hub_hash'
        self.db_handler.create_table(sat_schema.sat_name, sat_schema.columns)

    def create_link(self, link_schema: LinkSchema):
        for hub_name in link_schema.hub_names:
            link_schema.columns.append(ColumnSchema(name=f'{hub_name}_hash', type=str, foreign_key=f'{hub_name}.hub_hash'))
        link_schema.columns.extend(link_schema.additional_columns)
        self.db_handler.create_table(link_schema.link_name, link_schema.columns)