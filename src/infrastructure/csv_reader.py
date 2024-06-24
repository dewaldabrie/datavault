import csv
import json
from typing import Any, Dict, List
from src.domain.interfaces import FileReader

class CSVReader(FileReader):
    def read_schema(self, path: str) -> List[Dict[str, Any]]:
        with open(path, 'r') as file:
            return json.load(file)

    def read_data(self, path: str) -> List[Dict[str, Any]]:
        with open(path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            return [row for row in reader]