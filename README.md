# ppd-epc-h3

A fully open, reproducible Python pipeline that links every residential property sale in England and Wales to its Energy Performance Certificate, adds ONS lat/longs, and indexes each transaction into Uber H3 hexagonal cells.

The output is a single Parquet mart — partitioned by year, compressed with Zstandard — covering 29.4 million Category A transactions from 1995 to 2026, with 70.4% matched to an EPC and 99.9% geocoded.

**Used by:** [PropMap](https://github.com/fuzzykobe/ppd-epc-h3-viz) — an interactive property-market analytics platform that consumes these Parquet marts to serve a FastAPI + DuckDB backend and a React/DeckGL/MapLibre frontend.

---

## What it does

Three open-government datasets describe the same housing stock from different angles:

| Dataset | What it contains |
|---|---|
| **HM Land Registry PPD** | Every residential sale: price, date, address, tenure, property type |
| **MHCLG Domestic EPC** | Energy certificates: floor area, construction age, heating type, EPC rating (A–G), CO₂ emissions, 50+ more fields |
| **ONS Postcode Directory** | Postcode → lat/lon, Output Area, LSOA, MSOA, Local Authority, Region, IMD rank |

This pipeline joins them together. PPD and EPC use different address formats, so the join uses a two-phase strategy:

1. **Exact match** — normalise both address strings (uppercase, strip punctuation, collapse whitespace), then join within postcode blocks. Matches 54% of transactions.
2. **Fuzzy match** — for unmatched rows, apply `token_sort_ratio` (rapidfuzz, threshold 85) across all EPC candidates in the same postcode. Adds another 16%.

**Combined match rate from a full run (May 2026): 70.4%** across all years. Post-2008 transactions where EPC coverage exists match at a higher rate; pre-2008 transactions are structurally unmatchable unless the property was later re-assessed.

Each matched transaction is then:
- Enriched with ONS lat/long and geographic codes (LSOA, MSOA, Local Authority, IMD rank)
- Indexed into [Uber H3](https://h3geo.org/) hexagonal cells at resolutions 7, 8, 9, and 10

Final output is written as Parquet partitioned by `transfer_year`, one file per year.

---

## Data volume

| Metric | Value |
|---|---|
| PPD Category A transactions | 29,429,065 |
| Date range | 1995-01-01 → 2026-03-31 |
| Output mart size | 2.67 GB (32 year-partition files) |
| Median sale price | £156,995 |

## Match rate

| Stage | Count | Share |
|---|---|---|
| Exact matches | 16,003,795 | 54.4% |
| Fuzzy matches | 4,704,430 | 16.0% |
| **Total matched** | **20,708,225** | **70.4%** |
| Unmatched | 8,720,840 | 29.6% |
| Geo-enriched (lat/lon present) | 29,395,660 | 99.94% |
| H3 indexed | 29,395,660 | 99.94% |

---

## Output schema (key fields)

```
mart_transactions/transfer_year=YYYY/data.parquet

-- Identity
transaction_id, lmk_key, match_type, match_score, epc_before_transaction

-- Price Paid fields
price, transfer_date, postcode, property_type, old_new, duration
paon, saon, street, town_city, district, county_ppd

-- EPC fields (null if unmatched)
current_energy_rating, current_energy_efficiency   -- e.g. 'D', 68
total_floor_area, construction_age_band
epc_property_type, epc_built_form
co2_emissions_current, energy_consumption_current
mains_gas_flag, main_fuel, solar_water_heating_flag
heating_cost_current, walls_description, roof_description, ...

-- ONS geography
lat, lon, lsoa21, msoa21, ladcd, rgn, ctry, imd_rank

-- H3 spatial index
h3_r7, h3_r8, h3_r9, h3_r10
```

Full schema: see [`pipeline/06_enrich_geo.py`](pipeline/06_enrich_geo.py) and [`sql/mart_transactions.sql`](sql/mart_transactions.sql).

---

## Pipeline step timings (M4 Mac Mini, 16-core, 48 GB)

| Step | Script | Description | Time |
|---|---|---|---|
| 1 | `01_ingest_ppd.py` | `pp-complete.csv` → `stg_ppd.parquet` | ~9.5s |
| 2 | `02_ingest_epc.py` | `epc/**/*.csv` → `stg_epc.parquet` (deduped) | ~70s |
| 3 | `03_ingest_ons.py` | `ONSPD_latest.csv` → `stg_ons.parquet` | ~1s |
| 4 | `04_normalise.py` | Validates `addr_key` on both staging tables | <1s |
| 5 | `05_match.py` | PPD + EPC → `matches_all.parquet` | ~71s |
| 6 | `06_enrich_geo.py` | PPD + matches + EPC + ONS → `transactions_geo.parquet` | ~12s |
| 7 | `07_apply_h3.py` | `transactions_geo` → `transactions_h3.parquet` | ~77s |
| 8 | `08_build_mart.py` | `transactions_h3` → `outputs/mart_transactions/` | ~2m30s |
| — | Crime + income steps | `09`–`13` ingest, aggregate, build mart | ~30s |
| **Total** | | | **~5 min** |

End-to-end runtime (excluding data download): **~5 minutes**.

---

## Data sources

All three core datasets are free and licensed under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). See [`LICENCES_DATA.md`](LICENCES_DATA.md) for full attribution.

### 1. HM Land Registry — Price Paid Data

- **URL:** https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
- Download **"complete dataset (single file)"** → `pp-complete.csv` (~4 GB, ~30M rows, no login required)

```bash
wget -O data/raw/ppd/pp-complete.csv \
  "https://cf.mastodon.green/media_proxy/cached/pp-complete.csv"
```

### 2. MHCLG — Domestic Energy Performance Certificates

> **New service:** The original `epc.opendatacommunities.org` **retired on 30 May 2026**.  
> Use the replacement MHCLG "Get energy performance of buildings data" service:  
> **https://get-energy-performance-data.communities.gov.uk**  
> Download the bulk domestic dataset from there. `02_ingest_epc.py` is compatible as long as CSV headers match.

- Login required (free registration)
- Download the full **domestic bulk ZIP**
- Extract so that `certificates.csv` files live under `data/raw/epc/<local-authority>/certificates.csv`

### 3. ONS Postcode Directory (ONSPD)

- **URL:** https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory
- Download the latest edition ZIP, extract, and place the single combined CSV at `data/raw/ons/ONSPD_latest.csv`

---

## Setup

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
# Install uv if needed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install dependencies
git clone https://github.com/fuzzykobe/ppd-epc-h3.git
cd ppd-epc-h3
uv sync
```

Verify you have native ARM64 binaries (important on Apple Silicon):

```bash
uv run python -c "import platform; print(platform.machine())"
# Expected: arm64
```

Place the three datasets in the correct locations (see Data Sources above), then:

```
data/
├── raw/
│   ├── ppd/pp-complete.csv
│   ├── epc/<local-authority-code>/certificates.csv   (×347 directories)
│   └── ons/ONSPD_latest.csv
```

---

## Running the pipeline

```bash
# Run all steps end-to-end
uv run python pipeline/run_all.py

# Resume from a specific step (e.g. after tweaking the match threshold)
uv run python pipeline/run_all.py --from-step 5

# Run a single step
uv run python pipeline/01_ingest_ppd.py
uv run python pipeline/05_match.py
```

After a full run, `outputs/match_report.json` contains match rate and coverage statistics.

---

## Running tests

```bash
uv run pytest tests/ -v
```

27 unit tests covering address normalisation, exact/fuzzy matching logic, EPC deduplication, and H3 cell assignment.

---

## Hardware notes

The pipeline is optimised for **Apple Silicon (M-series)** with ≥ 16 GB unified memory. Tested on M4 Mac Mini (16-core CPU, 48 GB).

- All dependencies install as native `arm64` binaries — do not run under Rosetta
- DuckDB is configured with `PRAGMA threads=16` and `PRAGMA memory_limit='40GB'` for the join-heavy steps
- The fuzzy match step uses `ProcessPoolExecutor(max_workers=15)` — leaving one core free for DuckDB background threads
- The mart build step uses a separate 24 GB cap to avoid OOM during the partitioned Parquet write

On smaller machines, lower `DUCKDB_THREADS`, `DUCKDB_MEMORY`, and `FUZZY_MAX_WORKERS` in [`pipeline/config.py`](pipeline/config.py).

---

## Repository layout

```
ppd-epc-h3/
├── pipeline/
│   ├── config.py          # paths, DuckDB settings, H3 resolutions
│   ├── 01_ingest_ppd.py
│   ├── 02_ingest_epc.py
│   ├── 03_ingest_ons.py
│   ├── 04_normalise.py    # addr_key validation
│   ├── 05_match.py        # exact + fuzzy address matching
│   ├── 06_enrich_geo.py   # join to ONS lat/lon
│   ├── 07_apply_h3.py     # H3 spatial indexing
│   ├── 08_build_mart.py   # partitioned Parquet output
│   └── run_all.py         # orchestrator with --from-step flag
├── sql/                   # DuckDB SQL templates for each stage
├── tests/                 # unit tests + fixture CSVs
├── data/                  # gitignored — user downloads here
└── outputs/               # gitignored — generated Parquet
```

---

## Methodology references

- [Centre for Net Zero — epc-ppd-address-matching](https://github.com/centrefornetzero/epc-ppd-address-matching) — rule-based Python matching, Parquet-native
- [Bin-Chi — Link-LR-PPD-and-Domestic-EPCs](https://github.com/Bin-Chi/Link-LR-PPD-and-Domestic-EPCs) — academic R pipeline, 93.15% match rate benchmark (post-2008 matched properties only)

---

## Versioning

This project follows [Semantic Versioning](https://semver.org/). See [CHANGELOG.md](CHANGELOG.md) for the full history.
