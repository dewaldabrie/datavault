from dataclasses import dataclass
from typing import List

@dataclass
class FieldSchema:
    fieldname: str
    python_type: str
    db_type: str
    db_col_length: int
    description: str

@dataclass
class DataSchema:
    fields: List[FieldSchema]