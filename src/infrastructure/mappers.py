from typing import List
from src.contexts.data_landing.domain.models import FieldSchema, DataSchema
from src.contexts.data_vault.domain.models import ColumnSchema as DomainColumnSchema, HubSchema as DomainHubSchema, SatelliteSchema as DomainSatelliteSchema, LinkSchema as DomainLinkSchema
from src.infrastructure.models import ColumnSchema as InfraColumnSchema, HubSchema as InfraHubSchema, SatelliteSchema as InfraSatelliteSchema, LinkSchema as InfraLinkSchema



def map_field_schema_to_column_schema(field_schema: FieldSchema) -> InfraColumnSchema:
    return InfraColumnSchema(
        name=field_schema.fieldname,
        type=field_schema.db_type,
        type_length=field_schema.db_col_length,
        description=field_schema.description
    )

def map_data_schema_to_column_schemas(data_schema: DataSchema) -> List[InfraColumnSchema]:
    return [map_field_schema_to_column_schema(field) for field in data_schema.fields]


def map_column_schema_to_infra(column_schema: DomainColumnSchema) -> InfraColumnSchema:
    return InfraColumnSchema(
        name=column_schema.name,
        type=column_schema.type,
        type_length=column_schema.type_length,
        description=column_schema.description,
        primary_key=column_schema.primary_key,
        nullable=column_schema.nullable,
        unique=column_schema.unique,
        foreign_key=column_schema.foreign_key
    )

def map_hub_schema_to_infra(hub_schema: DomainHubSchema) -> InfraHubSchema:
    return InfraHubSchema(
        hub_name=hub_schema.hub_name,
        business_key=hub_schema.business_key.name,
        columns=[map_column_schema_to_infra(col) for col in hub_schema.columns]
    )

def map_satellite_schema_to_infra(sat_schema: DomainSatelliteSchema) -> InfraSatelliteSchema:
    return InfraSatelliteSchema(
        sat_name=sat_schema.sat_name,
        hub_name=sat_schema.hub_name,
        columns=[map_column_schema_to_infra(col) for col in sat_schema.columns]
    )

def map_link_schema_to_infra(link_schema: DomainLinkSchema) -> InfraLinkSchema:
    return InfraLinkSchema(
        link_name=link_schema.link_name,
        hub_names=link_schema.hub_names,
        additional_columns=[map_column_schema_to_infra(col) for col in link_schema.additional_columns],
        columns=[map_column_schema_to_infra(col) for col in link_schema.columns]
    )