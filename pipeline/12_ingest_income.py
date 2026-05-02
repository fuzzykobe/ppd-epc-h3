"""
Ingest ONS small-area household income estimates.

Files must be placed manually in data/raw/income/ (see README there).
Supports Excel (.xlsx/.xls) and CSV; auto-detects financial year from filename.
Normalises column names with fuzzy matching across ONS format changes.
Joins to stg_ons on msoa21 code to carry through rgn/ladcd for downstream joins.
Writes data/staged/stg_income.parquet (long format: one row per MSOA × year).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import click
import duckdb
from loguru import logger

from pipeline.config import DATA_RAW, DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS

INCOME_DIR = DATA_RAW / "income"

# ─── Column-name aliases (lowercase, stripped) ────────────────────────────────
# Keyed by canonical name → list of patterns that match that column
_COL_ALIASES: dict[str, list[str]] = {
    "msoa_code": [
        "msoa code", "msoa11 code", "msoa21 code", "msoacd", "area code",
        "msoa", "code",
    ],
    "msoa_name": [
        "msoa name", "msoa11 name", "msoa21 name", "msoaname", "area name",
        "name",
    ],
    "la_code":   ["la code", "ladcd", "local authority code", "lad code"],
    "la_name":   ["la name", "local authority name", "lad name", "ladnm"],
    "rgn_code":  ["region code", "rgn", "region", "gor code"],
    "rgn_name":  ["region name", "region_name", "gor name"],
    "net_income_mean": [
        "net income (£) mean", "net income mean", "mean net income",
        "mean (£)", "mean", "net annual income mean",
        "net income estimate (mean) (£)",
        "total net income: mean (£)",
        "net household income mean (£)",
        "mean net annual income",
    ],
    "net_income_median": [
        "net income (£) median", "net income median", "median net income",
        "median (£)", "median", "net annual income median",
        "net income estimate (median) (£)",
        "total net income: median (£)",
        "net household income median (£)",
        "median net annual income",
    ],
}


def _match_col(raw: str) -> str | None:
    """Return canonical name for a raw header, or None."""
    key = raw.strip().lower()
    for canonical, aliases in _COL_ALIASES.items():
        if any(key == a or key.startswith(a) for a in aliases):
            return canonical
    return None


def _year_from_path(p: Path) -> int | None:
    """Extract first 4-digit year from filename stem (e.g. 2021 from 'net2021to2022')."""
    m = re.search(r"20(\d{2})", p.stem)
    if m:
        return int("20" + m.group(1))
    return None


def _read_excel(path: Path) -> list[dict[str, Any]]:
    """Read ONS income Excel file; tries each sheet until columns are found."""
    try:
        import openpyxl
    except ImportError:
        logger.error("openpyxl not installed — run: pip install openpyxl")
        sys.exit(1)

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5:
            continue

        # Find header row: first row where ≥3 cells are non-empty strings
        header_idx = None
        for i, row in enumerate(rows[:20]):
            non_empty = [c for c in row if c and str(c).strip()]
            if len(non_empty) >= 3:
                header_idx = i
                break
        if header_idx is None:
            continue

        raw_headers = [str(c).strip() if c else "" for c in rows[header_idx]]
        col_map = {}
        for i, h in enumerate(raw_headers):
            canon = _match_col(h)
            if canon and canon not in col_map:
                col_map[canon] = i

        required = {"msoa_code", "net_income_mean"}
        if not required.issubset(col_map):
            logger.debug("Sheet '{}' missing required cols (found: {})", sheet_name, list(col_map))
            continue

        records: list[dict[str, Any]] = []
        for row in rows[header_idx + 1:]:
            if not any(row):
                continue
            msoa = row[col_map["msoa_code"]]
            if not msoa or not re.match(r"^[EW]\d{8}$", str(msoa).strip()):
                continue
            mean_inc   = row[col_map["net_income_mean"]]
            median_inc = row[col_map.get("net_income_median", col_map["net_income_mean"])]

            def _to_float(v: Any) -> float | None:
                if v is None:
                    return None
                try:
                    f = float(str(v).replace(",", "").replace("£", "").strip())
                    return f if f > 0 else None
                except (ValueError, TypeError):
                    return None

            rec: dict[str, Any] = {
                "msoa_code":        str(msoa).strip(),
                "msoa_name":        str(row[col_map["msoa_name"]]).strip() if "msoa_name" in col_map else None,
                "la_code":          str(row[col_map["la_code"]]).strip()   if "la_code"   in col_map else None,
                "la_name":          str(row[col_map["la_name"]]).strip()   if "la_name"   in col_map else None,
                "rgn_code":         str(row[col_map["rgn_code"]]).strip()  if "rgn_code"  in col_map else None,
                "rgn_name":         str(row[col_map["rgn_name"]]).strip()  if "rgn_name"  in col_map else None,
                "net_income_mean":   _to_float(mean_inc),
                "net_income_median": _to_float(median_inc),
            }
            if rec["net_income_mean"] is not None:
                records.append(rec)

        if records:
            logger.info("  Sheet '{}': {:,} rows", sheet_name, len(records))
            wb.close()
            return records

    wb.close()
    return []


def _read_csv(path: Path) -> list[dict[str, Any]]:
    """Fallback: read CSV with auto-detected columns."""
    import csv
    with open(path, encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        raw_headers = reader.fieldnames or []
        col_map = {}
        for h in raw_headers:
            canon = _match_col(h)
            if canon and canon not in col_map:
                col_map[canon] = h

        if "msoa_code" not in col_map or "net_income_mean" not in col_map:
            return []

        records = []
        for row in reader:
            msoa = row.get(col_map["msoa_code"], "").strip()
            if not re.match(r"^[EW]\d{8}$", msoa):
                continue

            def _get(k: str) -> str | None:
                return row.get(col_map[k], "").strip() if k in col_map else None

            def _to_float(v: str | None) -> float | None:
                if not v:
                    return None
                try:
                    f = float(v.replace(",", "").replace("£", "").strip())
                    return f if f > 0 else None
                except ValueError:
                    return None

            rec: dict[str, Any] = {
                "msoa_code":        msoa,
                "msoa_name":        _get("msoa_name"),
                "la_code":          _get("la_code"),
                "la_name":          _get("la_name"),
                "rgn_code":         _get("rgn_code"),
                "rgn_name":         _get("rgn_name"),
                "net_income_mean":   _to_float(row.get(col_map["net_income_mean"])),
                "net_income_median": _to_float(row.get(col_map.get("net_income_median", ""))),
            }
            if rec["net_income_mean"] is not None:
                records.append(rec)
        return records


@click.command()
@click.option("--income-dir", default=None, type=click.Path(path_type=Path),
              help="Override default data/raw/income directory")
def main(income_dir: Path | None) -> None:
    src_dir = income_dir or INCOME_DIR
    out = DATA_STAGED / "stg_income.parquet"
    ons_parquet = DATA_STAGED / "stg_ons.parquet"

    if not src_dir.exists():
        raise FileNotFoundError(f"Income directory not found: {src_dir}")

    files = sorted(
        [p for p in src_dir.iterdir() if p.suffix.lower() in {".xlsx", ".xls", ".csv"}]
    )
    if not files:
        raise FileNotFoundError(
            f"No .xlsx/.xls/.csv files found in {src_dir}\n"
            "Download ONS income data and place in that directory — see README.md there."
        )

    logger.info(f"Found {len(files)} income file(s) in {src_dir}")

    all_rows: list[dict[str, Any]] = []
    for path in files:
        year = _year_from_path(path)
        if year is None:
            logger.warning(f"  Could not detect year from filename: {path.name} — skipping")
            continue

        logger.info(f"  {path.name} → financial_year={year}")
        if path.suffix.lower() in {".xlsx", ".xls"}:
            rows = _read_excel(path)
        else:
            rows = _read_csv(path)

        if not rows:
            logger.warning(f"  No usable rows extracted from {path.name}")
            continue

        for r in rows:
            r["financial_year"] = year
        all_rows.extend(rows)
        logger.success(f"  {path.name}: {len(rows):,} MSOAs")

    if not all_rows:
        raise ValueError("No income data could be parsed from any file. Check README for format.")

    logger.info(f"Total rows before dedup: {len(all_rows):,}")

    # Write to parquet via DuckDB (handles type inference cleanly)
    import json
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    # Register as in-memory table from Python list
    con.execute("CREATE TABLE raw_income AS SELECT * FROM (VALUES (1)) t(x) WHERE false")
    con.execute("""
        CREATE TABLE raw_income (
            msoa_code         VARCHAR,
            msoa_name         VARCHAR,
            la_code           VARCHAR,
            la_name           VARCHAR,
            rgn_code          VARCHAR,
            rgn_name          VARCHAR,
            net_income_mean   DOUBLE,
            net_income_median DOUBLE,
            financial_year    INTEGER
        )
    """)

    # Insert via duckdb parameter binding in batches
    batch_size = 10_000
    for i in range(0, len(all_rows), batch_size):
        batch = all_rows[i:i + batch_size]
        tuples = [
            (
                r["msoa_code"], r.get("msoa_name"), r.get("la_code"), r.get("la_name"),
                r.get("rgn_code"), r.get("rgn_name"),
                r["net_income_mean"], r.get("net_income_median"),
                r["financial_year"],
            )
            for r in batch
        ]
        con.executemany(
            "INSERT INTO raw_income VALUES (?,?,?,?,?,?,?,?,?)",
            tuples,
        )

    # Deduplicate (keep last seen for each msoa × year)
    dedup_count = con.execute("SELECT COUNT(*) FROM raw_income").fetchone()[0]
    logger.info(f"Inserted {dedup_count:,} rows; deduplicating on msoa_code × financial_year…")

    # Enrich with rgn from stg_ons if available (better coverage than ONS income file column)
    if ons_parquet.exists():
        logger.info("Enriching with rgn/ladcd from stg_ons…")
        con.execute(f"""
            CREATE TABLE stg_income AS
            SELECT
                r.msoa_code,
                r.msoa_name,
                COALESCE(o_agg.ladcd, r.la_code)   AS ladcd,
                COALESCE(o_agg.rgn,   r.rgn_code)  AS rgn,
                r.net_income_mean,
                r.net_income_median,
                r.financial_year
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY msoa_code, financial_year
                                       ORDER BY net_income_mean DESC) AS rn
                FROM raw_income
            ) r
            LEFT JOIN (
                SELECT msoa21, ANY_VALUE(ladcd) AS ladcd, ANY_VALUE(rgn) AS rgn
                FROM read_parquet('{ons_parquet}')
                WHERE msoa21 IS NOT NULL
                GROUP BY msoa21
            ) o_agg ON r.msoa_code = o_agg.msoa21
            WHERE r.rn = 1
              AND r.net_income_mean IS NOT NULL
        """)
    else:
        logger.warning("stg_ons.parquet not found — rgn enrichment skipped")
        con.execute("""
            CREATE TABLE stg_income AS
            SELECT
                msoa_code, msoa_name,
                la_code AS ladcd, rgn_code AS rgn,
                net_income_mean, net_income_median, financial_year
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY msoa_code, financial_year
                                       ORDER BY net_income_mean DESC) AS rn
                FROM raw_income
            ) WHERE rn = 1 AND net_income_mean IS NOT NULL
        """)

    DATA_STAGED.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY stg_income TO '{out}'
        (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6)
    """)

    final_count = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    years = con.execute(f"SELECT MIN(financial_year), MAX(financial_year) FROM '{out}'").fetchone()
    logger.success(f"stg_income.parquet written — {final_count:,} rows, years {years[0]}–{years[1]}")


if __name__ == "__main__":
    main()
