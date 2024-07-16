from typing import List
from src.contexts.data_landing.domain.models import FieldSchema, DataSchema
from src.infrastructure.models import ColumnSchema as InfraColumnSchema



def map_field_schema_to_column_schema(field_schema: FieldSchema) -> InfraColumnSchema:
    return InfraColumnSchema(
        name=field_schema.fieldname,
        type=field_schema.db_type,
        type_length=field_schema.db_col_length,
        description=field_schema.description
    )

def map_data_schema_to_column_schemas(data_schema: DataSchema) -> List[InfraColumnSchema]:
    return [map_field_schema_to_column_schema(field) for field in data_schema.fields]

