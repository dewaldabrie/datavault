import os

def create_initial_files(base_path):
    # Mapping of paths to their initial content
    file_structure = {
        "src/domain/models.py": "class BaseModel:\n    pass\n",
        "src/application/service.py": "def main_service_function():\n    pass\n",
        "src/infrastructure/database.py": "from sqlalchemy import create_engine\n\ndef get_engine():\n    return create_engine('sqlite:///:memory:')\n",
        "src/contexts/data_landing/processor.py": "def process_landing_data():\n    pass\n",
        "src/contexts/data_vault_table_creation/creator.py": "def create_vault_tables():\n    pass\n",
        "src/contexts/data_vault_data_insertion/inserter.py": "def insert_into_vault():\n    pass\n",
        "src/contexts/business_vault_inference/inferencer.py": "def infer_business_data():\n    pass\n",
        "tests/domain/test_models.py": "def test_base_model():\n    assert True\n",
        "tests/application/test_service.py": "def test_main_service_function():\n    assert True\n",
        "tests/infrastructure/test_database.py": "def test_get_engine():\n    assert True\n",
        "tests/contexts/data_landing/test_processor.py": "def test_process_landing_data():\n    assert True\n",
        "tests/contexts/data_vault_table_creation/test_creator.py": "def test_create_vault_tables():\n    assert True\n",
        "tests/contexts/data_vault_data_insertion/test_inserter.py": "def test_insert_into_vault():\n    assert True\n",
        "tests/contexts/business_vault_inference/test_inferencer.py": "def test_infer_business_data():\n    assert True\n"
    }

    # Create files with initial content
    for path, content in file_structure.items():
        full_path = os.path.join(base_path, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w') as file:
            file.write(content)
        print(f"Created file: {full_path}")

# Usage
create_initial_files("./")