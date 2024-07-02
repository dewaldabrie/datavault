from typing import Protocol, List, Dict, Any

class FileReader(Protocol):
    def read_schema(self, path: str) -> List[Dict[str, Any]]:
        ...

    def read_data(self, path: str) -> List[Dict[str, Any]]:
        ...
