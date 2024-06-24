from src.infrastructure.sqlalchemy_handler import SQLAlchemyHandler
from src.data_vault_table_creation import DataVaultTableCreation
from src.data_vault_data_insertion import DataVaultDataInsertion

# Initialize handlers
db_handler = SQLAlchemyHandler(database_url="your_database_url")
table_creator = DataVaultTableCreation(db_handler)
data_inserter = DataVaultDataInsertion(db_handler)

# Create tables
table_creator.create_hub(hub_name="instrument_hub", business_key="business_key")
table_creator.create_satelite(sat_name="instrument_satelite", hub_name="instrument_hub")
table_creator.create_link(link_name="instrument_same_as_link", hub_names=["instrument_hub"], additional_columns=[{"name": "alternative_business_key", "type": String(50)}])
table_creator.create_link(link_name="instrument_hierarchical_link", hub_names=["parent_hub", "child_hub"], additional_columns=[{"name": "relationship_type", "type": String(50)}])

# Insert data
hub_data = [
    {"business_key": "key1", "created_ts": datetime.utcnow(), "record_source": "source1"},
    # ... more data ...
]
data_inserter.insert_hub_data("instrument_hub", hub_data)

sat_data = [
    {"hub_hash": "hash1", "created_ts": datetime.utcnow(), "record_source": "source1", "attributes": {"attr1": "value1"}},
    # ... more data ...
]
data_inserter.insert_satelite_data("instrument_satelite", sat_data)

# Extend hash column lengths
db_handler.extend_column_length("instrument_hub", "hub_hash", 64)
db_handler.extend_column_length("instrument_satelite", "hash_diff", 64)