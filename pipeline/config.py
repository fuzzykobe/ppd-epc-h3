from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_STAGED = ROOT / "data" / "staged"
OUTPUTS = ROOT / "outputs"

PPD_CSV = DATA_RAW / "ppd" / "pp-complete.csv"
EPC_DIR = DATA_RAW / "epc"
ONS_CSV = DATA_RAW / "ons" / "ONSPD_latest.csv"
CRIME_DIR  = DATA_RAW / "crime"
INCOME_DIR = DATA_RAW / "income"
CRIME_KEEP_TYPES: list[str] | None = None  # None = keep all; set to filter at ingest

H3_RESOLUTIONS = [7, 8, 9, 10]

DUCKDB_THREADS = 16
DUCKDB_MEMORY = "40GB"

FUZZY_THRESHOLD = 85
FUZZY_MAX_WORKERS = 15  # leave 1 core free for DuckDB background threads

PPD_COLUMNS = [
    "transaction_id", "price", "transfer_date", "postcode",
    "property_type", "old_new", "duration", "paon", "saon",
    "street", "locality", "town_city", "district", "county",
    "ppd_category", "record_status",
]

EPC_KEEP_COLUMNS = [
    "LMK_KEY", "ADDRESS1", "ADDRESS2", "ADDRESS3", "POSTCODE", "POSTTOWN",
    "UPRN", "UPRN_SOURCE", "LODGEMENT_DATE", "LODGEMENT_DATETIME",
    "INSPECTION_DATE", "TRANSACTION_TYPE",
    "PROPERTY_TYPE", "BUILT_FORM", "CONSTRUCTION_AGE_BAND",
    "TOTAL_FLOOR_AREA", "NUMBER_HABITABLE_ROOMS", "NUMBER_HEATED_ROOMS",
    "FLOOR_LEVEL", "FLAT_STOREY_COUNT", "FLAT_TOP_STOREY", "EXTENSION_COUNT",
    "CURRENT_ENERGY_RATING", "POTENTIAL_ENERGY_RATING",
    "CURRENT_ENERGY_EFFICIENCY", "POTENTIAL_ENERGY_EFFICIENCY",
    "ENVIRONMENT_IMPACT_CURRENT", "CO2_EMISSIONS_CURRENT",
    "CO2_EMISS_CURR_PER_FLOOR_AREA", "ENERGY_CONSUMPTION_CURRENT",
    "MAINS_GAS_FLAG", "MAIN_FUEL", "SOLAR_WATER_HEATING_FLAG",
    "MECHANICAL_VENTILATION", "TENURE", "REPORT_TYPE",
    "HEATING_COST_CURRENT", "LIGHTING_COST_CURRENT", "HOT_WATER_COST_CURRENT",
    "WALLS_DESCRIPTION", "WALLS_ENERGY_EFF",
    "ROOF_DESCRIPTION", "ROOF_ENERGY_EFF",
    "WINDOWS_DESCRIPTION", "WINDOWS_ENERGY_EFF",
    "MAINHEAT_DESCRIPTION", "MAINHEAT_ENERGY_EFF",
    "HOTWATER_DESCRIPTION", "HOT_WATER_ENERGY_EFF",
    "LIGHTING_DESCRIPTION", "LIGHTING_ENERGY_EFF",
    "LOCAL_AUTHORITY", "LOCAL_AUTHORITY_LABEL", "COUNTY",
]

ONS_KEEP_COLUMNS = [
    "pcds", "lat", "long",
    "oa21", "lsoa21", "msoa21",
    "oslaua", "rgn", "ctry", "imd",
]
