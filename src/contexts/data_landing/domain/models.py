from typing import List, Dict, Any
from pydantic import BaseModel, create_model
from src.contexts.root.domain.models import DataSchema

def create_record_model(schema: DataSchema) -> BaseModel:
    fields = {field.fieldname: (eval(field.python_type), ...) for field in schema.fields}
    return create_model('Record', **fields)