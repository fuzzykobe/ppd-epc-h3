# ppd-epc-h3

A fully open, reproducible Python pipeline that joins HM Land Registry Price Paid Data (PPD) with Domestic EPC certificates, enriches with ONS lat/longs, and applies Uber H3 spatial indexing.

## What it produces

`outputs/mart_transactions/` — Parquet files partitioned by `transfer_year`, ZSTD-compressed, containing ~27M residential transactions enriched with:
- EPC energy performance data (current rating, floor area, construction age, heating type, etc.)
- ONS geography (LSOA, MSOA, LAD, region)
- H3 spatial indexes at resolutions 7, 8, 9, 10

Typical match rate: ~87% of Category A PPD transactions matched to an EPC certificate.

## Setup

```bash
# Install uv if needed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

## Data download (required before running pipeline)

All datasets are free, open-licensed (OGL v3). See [LICENCES_DATA.md](LICENCES_DATA.md).

### 1. Price Paid Data (PPD)
```bash
# ~4GB CSV — no authentication required
wget -O data/raw/ppd/pp-complete.csv \
  "https://cf.mastodon.green/media_proxy/cached/pp-complete.csv"
# Official source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
# Download "complete dataset (single file)" → pp-complete.csv
```

### 2. EPC Domestic Bulk
1. Register (free) at https://epc.opendatacommunities.org/
   - **Note:** this service retires 30 May 2026; new service from MHCLG TBC
2. Download the full domestic bulk ZIP
3. Extract all `certificates.csv` files into `data/raw/epc/`
   - Expected structure: `data/raw/epc/<local-authority-code>/certificates.csv`

### 3. ONS Postcode Directory (ONSPD)
1. Download from https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory
2. Extract and place the main CSV at `data/raw/ons/ONSPD_latest.csv`

## Running the pipeline

```bash
# Run all steps
uv run python pipeline/run_all.py

# Resume from a specific step (e.g. after fixing fuzzy match)
uv run python pipeline/run_all.py --from-step 5

# Run individual steps
uv run python pipeline/01_ingest_ppd.py
uv run python pipeline/02_ingest_epc.py
# etc.
```

## Pipeline steps

| Step | Script | Description |
|------|--------|-------------|
| 1 | `01_ingest_ppd.py` | Read PPD CSV → `stg_ppd.parquet` |
| 2 | `02_ingest_epc.py` | Read EPC CSVs, deduplicate → `stg_epc.parquet` |
| 3 | `03_ingest_ons.py` | Read ONSPD CSV → `stg_ons.parquet` |
| 4 | `04_normalise.py` | Add `addr_key` columns to PPD + EPC staging tables |
| 5 | `05_match.py` | Exact + fuzzy address matching → `matches_all.parquet` |
| 6 | `06_enrich_geo.py` | Join PPD + EPC + ONS, resolve cert dates → `transactions_geo.parquet` |
| 7 | `07_apply_h3.py` | Compute H3 cells at r7–r10 |
| 8 | `08_build_mart.py` | Write final partitioned mart |

## Match report

After each run, `outputs/match_report.json` records match rates and coverage stats.

## Running tests

```bash
uv run pytest tests/ -v
```

## Repository structure

```
ppd-epc-h3/
├── pipeline/        # Python pipeline scripts
├── sql/             # DuckDB SQL templates
├── tests/           # Unit tests + fixtures
├── data/            # gitignored — user downloads here
└── outputs/         # gitignored — generated Parquet
```
