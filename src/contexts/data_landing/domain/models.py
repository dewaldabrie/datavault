from typing import List
from pydantic import BaseModel, create_model

from typing import Any, List
from pydantic import BaseModel


class FieldSchema(BaseModel):
    fieldname: str
    python_type: str
    db_type: str
    db_col_length: int
    description: str

class DataSchema(BaseModel):
    fields: List[FieldSchema]


def create_record_model(schema: DataSchema) -> BaseModel:
    fields = {field.fieldname: (eval(field.python_type), ...) for field in schema.fields}
    return create_model('Record', **fields)