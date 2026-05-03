# Changelog — ppd-epc-h3

All notable changes are documented here. This project follows [Semantic Versioning](https://semver.org/).

---

## [1.0.1] — 2026-05-03

### Changed

- README: documented crime (steps 09–11) and income (steps 12–13) pipeline stages
- README: expanded pipeline steps table with output filenames, row counts, and timings for all 13 steps
- README: added Mart Outputs table (mart_transactions, mart_crime, mart_crime_monthly, mart_price_income)
- README: added PropMap consumption note with link

---

## [1.0.0] — 2026-05-03

### Added

**Core pipeline (steps 1–8)**
- PPD × EPC address matching pipeline: two-phase exact + fuzzy strategy (rapidfuzz `token_sort_ratio`, threshold 85)
- 70.4% overall match rate across 29.4 M Category A transactions (54.4% exact + 16.0% fuzzy)
- ONS Postcode Directory enrichment: lat/lon, LSOA, MSOA, Local Authority, Region, IMD rank; 99.94% geo-coverage
- H3 spatial indexing at resolutions 7, 8, 9, 10 for every transaction
- Output: Parquet mart partitioned by `transfer_year`, ZSTD-compressed, 2.67 GB for 1995–2026

**Crime pipeline (steps 9–11)**
- Police recorded crime ingestion from data.police.uk (monthly, LSOA level, Apr 2023–Mar 2026)
- 17.6 M crime records aggregated to H3-r8 cells
- `mart_crime` (annual totals by cell) and `mart_crime_monthly` (monthly by cell + category)

**Income pipeline (steps 12–13)**
- ONS MSOA household income estimates (2011–2023) ingested from Excel releases
- Linear gap interpolation for missing years within series
- Regional projection to 2026 using 2021–2023 CAGR per region
- `mart_price_income`: median price joined to MSOA-level income, aggregated to region × year

**Infrastructure**
- `run_all.py` orchestrator with `--from-step` resume flag
- DuckDB + Parquet + ZSTD throughout, optimised for Apple Silicon M4
- End-to-end runtime ~5 minutes on M4 Mac Mini (16-core, 48 GB), excluding data download
- 27 unit tests (address normalisation, matching logic, EPC deduplication, H3 assignment)
