from typing import List, Protocol
from src.contexts.data_vault.domain.models import HubSchema
from src.contexts.data_vault.domain.models import HubData

class DataVaultHandler(Protocol):
    def create(self, schema: HubSchema, drop_existing: bool = False):
        ...

    def populate(self, table_name: str, data: List[HubData]):
        ...

    def resize_hash_key(self, table_name: str, new_length: int):
        ...