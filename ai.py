import os

def create_project_structure(base_path):
    directories = [
        "src/domain",
        "src/application",
        "src/infrastructure",
        "src/contexts/data_landing",
        "src/contexts/data_vault_table_creation",
        "src/contexts/data_vault_data_insertion",
        "src/contexts/business_vault_inference",
        "tests/domain",
        "tests/application",
        "tests/infrastructure",
        "tests/contexts/data_landing",
        "tests/contexts/data_vault_table_creation",
        "tests/contexts/data_vault_data_insertion",
        "tests/contexts/business_vault_inference"
    ]
    
    # Create directories
    for directory in directories:
        dir_path = os.path.join(base_path, directory)
        os.makedirs(dir_path, exist_ok=True)
        print(f"Created directory: {dir_path}")
    
    # Create requirements.txt
    requirements_path = os.path.join(base_path, "requirements.txt")
    with open(requirements_path, "w") as f:
        f.write("sqlalchemy\n")
    print(f"Created file: {requirements_path}")

# Usage
create_project_structure("./")