import datetime
from src.infrastructure.sqlmodel_handler import SQLModelHandler
from src.contexts.data_vault.hub import HubHandler
from src.contexts.data_vault.satellite import SatelliteHandler
from src.contexts.data_vault.link import LinkHandler
from src.contexts.data_vault.domain.models import HubData, XDomainHubData, DocumentSatelliteData, LinkData
from datetime import datetime
from src.application.config import DB_URL


# Initialize handlers
db_handler = SQLModelHandler(database_url=DB_URL)

# Dependency injection
hub_handler = HubHandler(db_handler)
satellite_handler = SatelliteHandler(db_handler)
link_handler = LinkHandler(db_handler)




# Insert data
hub_data = [
    HubData(business_key="quote_id", created_ts=datetime.utcnow(), record_source="masterdata"),
    HubData(business_key="ric", created_ts=datetime.utcnow(), record_source="masterdata"),
    HubData(business_key="aaa_numeric_code", created_ts=datetime.utcnow(), record_source="masterdata"),
    HubData(business_key="bloomberg_global_id", created_ts=datetime.utcnow(), record_source="masterdata"),
]
hub_handler.populate("hub_sym_fam", hub_data)

hub_data = [
    XDomainHubData(business_key="ABC399L24.AAA", created_ts=datetime.utcnow(), record_source="DSP"),
    XDomainHubData(business_key="ABC.AAA", created_ts=datetime.utcnow(), record_source="DSP"),
    XDomainHubData(business_key="abc.aaa_", created_ts=datetime.utcnow(), record_source="BBGBO"),
]
hub_handler.populate("hub_ins_symbol", hub_data)

link_data = [
    LinkData(created_ts=datetime.utcnow(), record_source="DSP", hub_hashes={"hub_sym_fam": "hash2", "hub_ins_symbol": "hash3"}),
]
link_handler.populate("link_ins_sameas", link_data)

hub_data = [
    XDomainHubData(business_key="eq111", created_ts=datetime.utcnow(), record_source="DSP"),
    XDomainHubData(business_key="opt222", created_ts=datetime.utcnow(), record_source="DSP"),
    XDomainHubData(business_key="111eq", created_ts=datetime.utcnow(), record_source="BBGBO"),
]
hub_handler.populate("hub_ins", hub_data)



sat_dsp_opt_data = [
    DocumentSatelliteData(hub_hash="hash1", created_ts=datetime.utcnow(), record_source="DSP", attributes={"quote_id": "opt222", "exchange": "AAA", "underlying": "eq111", "ticker": "abc", "contract_size": 100, "strike": 399, "opt_type": "call", "exp_date": datetime.datetime(2024,12,31)}),
]
satellite_handler.populate("instrument_satelite", sat_dsp_opt_data)

sat_dsp_eq_data = [
    DocumentSatelliteData(hub_hash="hash2", created_ts=datetime.utcnow(), record_source="DSP", attributes={"quote_id": "eq111", "exchange": "AAA", "ticker": "abc"}),
]
satellite_handler.populate("instrument_satelite", sat_dsp_eq_data)

sat_bbgbo_eq_data = [
    DocumentSatelliteData(hub_hash="hash3", created_ts=datetime.utcnow(), record_source="BBGBO", attributes={"quote_id": "111eq", "exchange": "aaa_", "ticker": "abc"}),
]
satellite_handler.populate("instrument_satelite", sat_bbgbo_eq_data)


link_data = [
    # TODO: how to add data for addtional columns like "relationship_type" via LinkData model?
    LinkData(link_hash="link0", created_ts=datetime.utcnow(), record_source="DSP", hub_hashes={"hub_ins": "hash2", "hub_ins": "hash3"}),
]
link_handler.populate("link_ins_sameas", link_data)




link_data = [
    # TODO: how to add data for addtional columns like "relationship_type" via LinkData model?
    LinkData(link_hash="link1", created_ts=datetime.utcnow(), record_source="arbitrator", hub_hashes={"hub_ins": "hash2", "hub_ins": "hash3"}),
]
link_handler.populate("link_ins_sameas", link_data)

# Populate from staging
# db_handler.insert_data_from_staging(
#     target_table="HubData",
#     staging_table="staging_hub_table",
#     select_columns=['business_key', 'created_ts', 'record_source'],
#     transformations={'hub_hash': (HASH_KEY_FUNCTION, ['business_key'])}
# )

# db_handler.insert_data_from_staging(
#     target_table="DocumentSatelliteData",
#     staging_table="staging_satellite_table",
#     select_columns=['hub_hash', 'created_ts', 'record_source', 'attributes'],
#     transformations={'hash_diff': (HASH_KEY_FUNCTION, ['attributes'])}
# )

# db_handler.insert_data_from_staging(
#     target_table="LinkData",
#     staging_table="staging_link_table",
#     select_columns=['created_ts', 'record_source', 'hub_hashes'],
#     transformations={'link_hash': (HASH_KEY_FUNCTION, ['record_source', 'created_ts'])}
# )