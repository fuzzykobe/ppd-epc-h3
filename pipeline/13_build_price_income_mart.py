"""
Build price-to-income mart with gap-year interpolation and forward projection.

Pipeline:

  A. MSOA-level income interpolation
     ONS surveys: 2011, 2013, 2015, 2017, 2019, 2022 (start-of-FY convention).
     Linearly interpolates missing years per MSOA so the transaction JOIN has
     continuous coverage from 2011 to 2022.

  B. Main mart JOIN (2011–2022)
     Joins mart_transactions → income_expanded on msoa21 × financial_year.
     Aggregates median price, median income, PTI, FTB proxy per region × year.
     Flags: is_interpolated = year NOT IN actual survey years, is_projected = False.

  C. Regional projections (2023–2026)
     Fits a numpy linear trend per region on the 6 actual income survey years.
     Projects income forward; caps each projected value at ±15 % of the 2022 level.
     Uses ACTUAL transaction prices for 2023–2026 from mart_transactions.
     Flags: is_projected = True, is_interpolated = False.

  D. Write combined mart
     Output schema:
         year, rgn, region_name,
         median_price, median_income, price_to_income_ratio,
         ftb_proxy_price, ftb_price_to_income,
         n_transactions, is_interpolated, is_projected

     Partitioned by year, ZSTD compression.
"""
import click
import duckdb
import numpy as np
import pyarrow as pa
from collections import defaultdict
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS, OUTPUTS

INCOME_PARQUET    = DATA_STAGED / "stg_income.parquet"
MART_TRANSACTIONS = OUTPUTS / "mart_transactions"
MART_PRICE_INCOME = OUTPUTS / "mart_price_income"

# ONS income survey years stored in start-of-FY convention
# e.g. 2022 → FY 2022-23 (Apr 2022 – Mar 2023)
ACTUAL_INCOME_YEARS: frozenset[int] = frozenset({2011, 2013, 2015, 2017, 2019, 2022})

# Years to project (all 4 have actual transaction data as of 2026)
PROJECT_YEARS = [2023, 2024, 2025, 2026]

# Projection cap: ±15 % of 2022 (last actual) value
CAP_PCT = 0.15

RGN_NAMES: dict[str, str] = {
    "E12000001": "North East",
    "E12000002": "North West",
    "E12000003": "Yorkshire and The Humber",
    "E12000004": "East Midlands",
    "E12000005": "West Midlands",
    "E12000006": "East of England",
    "E12000007": "London",
    "E12000008": "South East",
    "E12000009": "South West",
    "W99999999": "Wales",
    "excl_london": "England & Wales excl. London",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _linear_project(
    xs: list[int],
    ys: list[float],
    target_x: int,
    anchor: float,
) -> float:
    """
    Fit a 1-degree polynomial to (xs, ys), evaluate at target_x,
    then clamp the result to ±CAP_PCT of anchor.
    """
    coeffs = np.polyfit(xs, ys, 1)
    raw = float(np.polyval(coeffs, target_x))
    lo = anchor * (1.0 - CAP_PCT)
    hi = anchor * (1.0 + CAP_PCT)
    return float(np.clip(raw, lo, hi))


# ── Main ─────────────────────────────────────────────────────────────────────

@click.command()
def main() -> None:
    if not INCOME_PARQUET.exists():
        raise FileNotFoundError(
            f"stg_income.parquet not found at {INCOME_PARQUET}.\n"
            "Run pipeline step 12 first (12_ingest_income.py)."
        )
    if not MART_TRANSACTIONS.exists():
        raise FileNotFoundError(
            f"mart_transactions not found at {MART_TRANSACTIONS}.\n"
            "Run pipeline steps 1–8 first."
        )

    MART_PRICE_INCOME.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    # ── Views ─────────────────────────────────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE VIEW transactions AS
        SELECT * FROM read_parquet('{MART_TRANSACTIONS}/**/*.parquet',
            hive_partitioning=true)
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW income_raw AS
        SELECT * FROM read_parquet('{INCOME_PARQUET}')
    """)

    # ── Diagnostics ───────────────────────────────────────────────────────────
    tx_msoa  = con.execute("SELECT COUNT(DISTINCT msoa21) FROM transactions WHERE msoa21 IS NOT NULL").fetchone()[0]
    inc_msoa = con.execute("SELECT COUNT(DISTINCT msoa_code) FROM income_raw").fetchone()[0]
    inc_span = con.execute("SELECT MIN(financial_year), MAX(financial_year) FROM income_raw").fetchone()
    logger.info(f"Transactions: {tx_msoa:,} distinct MSOAs")
    logger.info(f"Income: {inc_msoa:,} MSOAs, years {inc_span[0]}–{inc_span[1]}")

    raw_match = con.execute("""
        SELECT COUNT(DISTINCT t.msoa21) FROM transactions t
        JOIN income_raw i ON t.msoa21 = i.msoa_code
    """).fetchone()[0]
    logger.info(f"Direct MSOA match (pre-interpolation): {raw_match:,} / {tx_msoa:,}")

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION A: MSOA-level income interpolation
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("Interpolating MSOA income for gap years…")

    raw_rows = con.execute(
        "SELECT msoa_code, financial_year, net_income_median "
        "FROM income_raw ORDER BY msoa_code, financial_year"
    ).fetchall()

    # Build {msoa: {year: income}}
    msoa_series: dict[str, dict[int, float]] = defaultdict(dict)
    for msoa, year, income in raw_rows:
        msoa_series[msoa][int(year)] = float(income)

    # Expand with linear interpolation between consecutive known years
    expanded: list[tuple[str, int, float, bool]] = []
    for msoa, yr_inc in msoa_series.items():
        known = sorted(yr_inc)
        for i, y1 in enumerate(known):
            inc1 = yr_inc[y1]
            expanded.append((msoa, y1, inc1, False))          # actual
            if i + 1 < len(known):
                y2   = known[i + 1]
                inc2 = yr_inc[y2]
                for y in range(y1 + 1, y2):
                    frac = (y - y1) / (y2 - y1)
                    interp = inc1 + frac * (inc2 - inc1)
                    expanded.append((msoa, y, interp, True))  # interpolated

    logger.info(f"income_expanded: {len(expanded):,} rows after interpolation")

    # Register as an Arrow table so DuckDB can query it directly
    tbl = pa.table({
        "msoa_code":        pa.array([r[0] for r in expanded], pa.string()),
        "financial_year":   pa.array([r[1] for r in expanded], pa.int32()),
        "net_income_median": pa.array([r[2] for r in expanded], pa.float64()),
        "is_interpolated":  pa.array([r[3] for r in expanded], pa.bool_()),
    })
    con.register("income_expanded", tbl)

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION B: Main mart (2011–2022, actual + interpolated income years)
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("Building regional price-to-income aggregates (2011–2022)…")

    # Regional rows (England + Wales, excl. Scotland)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE mart AS
        WITH tx_income AS (
            SELECT
                t.transfer_year                 AS year,
                t.rgn,
                t.price,
                t.property_type,
                i.net_income_median             AS msoa_income_median,
                i.is_interpolated
            FROM transactions t
            JOIN income_expanded i
                ON  t.msoa21        = i.msoa_code
                AND t.transfer_year = i.financial_year
            WHERE t.rgn IS NOT NULL
              AND t.price > 10000
              AND t.rgn NOT LIKE 'S%'
              AND i.net_income_median > 1000
        ),
        all_props AS (
            SELECT year, rgn,
                MEDIAN(price)::DOUBLE              AS median_price,
                MEDIAN(msoa_income_median)::DOUBLE AS median_income,
                COUNT(*)                           AS n_transactions,
                -- year is interpolated if ANY income row is interpolated (all same within a year)
                BOOL_AND(is_interpolated)          AS is_interpolated
            FROM tx_income
            GROUP BY year, rgn
        ),
        ftb_props AS (
            SELECT year, rgn,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)::DOUBLE AS ftb_proxy_price
            FROM tx_income
            WHERE property_type IN ('F', 'T')
            GROUP BY year, rgn
        )
        SELECT
            a.year,
            a.rgn,
            a.median_price,
            a.median_income,
            a.median_price / NULLIF(a.median_income, 0)       AS price_to_income_ratio,
            f.ftb_proxy_price,
            f.ftb_proxy_price / NULLIF(a.median_income, 0)    AS ftb_price_to_income,
            a.n_transactions,
            a.is_interpolated,
            FALSE                                              AS is_projected
        FROM all_props a
        LEFT JOIN ftb_props f ON a.year = f.year AND a.rgn = f.rgn
        WHERE a.year BETWEEN 2011 AND 2022
          AND a.median_price  IS NOT NULL
          AND a.median_income IS NOT NULL
        ORDER BY a.year, a.rgn
    """)

    # National (excl. London) aggregate
    con.execute("""
        INSERT INTO mart
        WITH tx_income AS (
            SELECT
                t.transfer_year                 AS year,
                t.price,
                t.property_type,
                i.net_income_median             AS msoa_income_median,
                i.is_interpolated
            FROM transactions t
            JOIN income_expanded i
                ON  t.msoa21        = i.msoa_code
                AND t.transfer_year = i.financial_year
            WHERE t.rgn IS NOT NULL
              AND t.rgn NOT IN ('E12000007', 'S99999999')
              AND t.price > 10000
              AND i.net_income_median > 1000
        ),
        all_props AS (
            SELECT year,
                MEDIAN(price)::DOUBLE              AS median_price,
                MEDIAN(msoa_income_median)::DOUBLE AS median_income,
                COUNT(*)                           AS n_transactions,
                BOOL_AND(is_interpolated)          AS is_interpolated
            FROM tx_income
            WHERE year BETWEEN 2011 AND 2022
            GROUP BY year
        ),
        ftb_props AS (
            SELECT year,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)::DOUBLE AS ftb_proxy_price
            FROM tx_income
            WHERE property_type IN ('F', 'T')
              AND year BETWEEN 2011 AND 2022
            GROUP BY year
        )
        SELECT
            a.year,
            'excl_london'                                          AS rgn,
            a.median_price,
            a.median_income,
            a.median_price / NULLIF(a.median_income, 0)           AS price_to_income_ratio,
            f.ftb_proxy_price,
            f.ftb_proxy_price / NULLIF(a.median_income, 0)        AS ftb_price_to_income,
            a.n_transactions,
            a.is_interpolated,
            FALSE                                                  AS is_projected
        FROM all_props a
        LEFT JOIN ftb_props f ON a.year = f.year
        WHERE a.median_price IS NOT NULL
    """)

    count_base = con.execute("SELECT COUNT(*) FROM mart").fetchone()[0]
    years_base = con.execute("SELECT MIN(year), MAX(year) FROM mart").fetchone()
    logger.info(f"Base mart: {count_base} rows, years {years_base[0]}–{years_base[1]}")

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION C: Regional projections for 2023–2026
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("Projecting income 2023–2026 and joining to actual transaction prices…")

    # Pull actual regional data for trend fitting (actual survey years only)
    actual_rgn = con.execute("""
        SELECT year, rgn, median_price, median_income, ftb_proxy_price, n_transactions
        FROM mart
        WHERE NOT is_interpolated AND NOT is_projected
        ORDER BY rgn, year
    """).fetchall()

    # Group by region
    rgn_series: dict[str, dict[int, dict[str, float | None]]] = defaultdict(dict)
    for year, rgn, med_price, med_income, ftb_price, n_tx in actual_rgn:
        rgn_series[rgn][int(year)] = {
            "median_price":   float(med_price)  if med_price  is not None else None,
            "median_income":  float(med_income) if med_income is not None else None,
            "ftb_proxy_price": float(ftb_price) if ftb_price is not None else None,
        }

    # Fetch actual transaction-derived prices for 2023–2026 (no income join needed)
    actual_proj_prices: dict[str, dict[int, dict[str, float | None]]] = defaultdict(dict)

    proj_rows = con.execute("""
        WITH tx AS (
            SELECT transfer_year AS year, rgn, price, property_type
            FROM transactions
            WHERE transfer_year BETWEEN 2023 AND 2026
              AND rgn IS NOT NULL
              AND rgn NOT LIKE 'S%'
              AND price > 10000
        ),
        all_props AS (
            SELECT year, rgn,
                MEDIAN(price)::DOUBLE AS median_price,
                COUNT(*)              AS n_transactions
            FROM tx GROUP BY year, rgn
        ),
        ftb_props AS (
            SELECT year, rgn,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)::DOUBLE AS ftb_proxy_price
            FROM tx WHERE property_type IN ('F', 'T') GROUP BY year, rgn
        )
        SELECT a.year, a.rgn, a.median_price, f.ftb_proxy_price, a.n_transactions
        FROM all_props a LEFT JOIN ftb_props f ON a.year = f.year AND a.rgn = f.rgn
    """).fetchall()

    for year, rgn, med_price, ftb_price, n_tx in proj_rows:
        actual_proj_prices[rgn][int(year)] = {
            "median_price":    float(med_price)  if med_price  is not None else None,
            "ftb_proxy_price": float(ftb_price)  if ftb_price is not None else None,
            "n_transactions":  int(n_tx),
        }

    # excl_london prices for 2023–2026
    excl_london_proj = con.execute("""
        WITH tx AS (
            SELECT transfer_year AS year, price, property_type
            FROM transactions
            WHERE transfer_year BETWEEN 2023 AND 2026
              AND rgn IS NOT NULL
              AND rgn NOT IN ('E12000007', 'S99999999')
              AND price > 10000
        ),
        all_props AS (
            SELECT year,
                MEDIAN(price)::DOUBLE AS median_price,
                COUNT(*)              AS n_transactions
            FROM tx GROUP BY year
        ),
        ftb_props AS (
            SELECT year,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)::DOUBLE AS ftb_proxy_price
            FROM tx WHERE property_type IN ('F', 'T') GROUP BY year
        )
        SELECT a.year, a.median_price, f.ftb_proxy_price, a.n_transactions
        FROM all_props a LEFT JOIN ftb_props f ON a.year = f.year
    """).fetchall()

    for year, med_price, ftb_price, n_tx in excl_london_proj:
        actual_proj_prices["excl_london"][int(year)] = {
            "median_price":    float(med_price)  if med_price  is not None else None,
            "ftb_proxy_price": float(ftb_price)  if ftb_price is not None else None,
            "n_transactions":  int(n_tx),
        }

    # Build projected rows per region
    proj_records: list[dict] = []
    all_regions = set(rgn_series) | set(actual_proj_prices)

    for rgn in all_regions:
        known_data = rgn_series.get(rgn, {})
        if not known_data:
            logger.warning(f"No actual data for region {rgn}, skipping projection")
            continue

        actual_yrs = sorted(known_data)
        incomes     = [known_data[y]["median_income"]   for y in actual_yrs if known_data[y]["median_income"] is not None]
        inc_yrs     = [y for y in actual_yrs            if known_data[y]["median_income"]   is not None]

        if len(inc_yrs) < 2:
            logger.warning(f"Insufficient income data for {rgn}, skipping projection")
            continue

        # Anchor = last actual income (2022)
        anchor_income = float(known_data[max(actual_yrs)]["median_income"])  # type: ignore[arg-type]

        for proj_yr in PROJECT_YEARS:
            price_data = actual_proj_prices.get(rgn, {}).get(proj_yr)
            if price_data is None:
                logger.warning(f"No transaction data for {rgn}/{proj_yr}, skipping")
                continue

            proj_income = _linear_project(inc_yrs, incomes, proj_yr, anchor_income)
            med_price   = price_data["median_price"]
            ftb_price   = price_data["ftb_proxy_price"]
            n_tx        = price_data["n_transactions"]

            if med_price is None:
                continue

            proj_records.append({
                "year":                  proj_yr,
                "rgn":                   rgn,
                "median_price":          med_price,
                "median_income":         proj_income,
                "price_to_income_ratio": med_price / proj_income if proj_income else None,
                "ftb_proxy_price":       ftb_price,
                "ftb_price_to_income":   (ftb_price / proj_income) if (ftb_price and proj_income) else None,
                "n_transactions":        n_tx,
                "is_interpolated":       False,
                "is_projected":          True,
            })

    logger.info(f"Projected rows: {len(proj_records)}")

    if proj_records:
        proj_tbl = pa.table({
            "year":                  pa.array([r["year"]                  for r in proj_records], pa.int32()),
            "rgn":                   pa.array([r["rgn"]                   for r in proj_records], pa.string()),
            "median_price":          pa.array([r["median_price"]          for r in proj_records], pa.float64()),
            "median_income":         pa.array([r["median_income"]         for r in proj_records], pa.float64()),
            "price_to_income_ratio": pa.array([r["price_to_income_ratio"] for r in proj_records], pa.float64()),
            "ftb_proxy_price":       pa.array([r["ftb_proxy_price"]       for r in proj_records], pa.float64()),
            "ftb_price_to_income":   pa.array([r["ftb_price_to_income"]   for r in proj_records], pa.float64()),
            "n_transactions":        pa.array([r["n_transactions"]        for r in proj_records], pa.int64()),
            "is_interpolated":       pa.array([r["is_interpolated"]       for r in proj_records], pa.bool_()),
            "is_projected":          pa.array([r["is_projected"]          for r in proj_records], pa.bool_()),
        })
        con.register("proj_rows", proj_tbl)
        con.execute("INSERT INTO mart SELECT * FROM proj_rows")

    # ─────────────────────────────────────────────────────────────────────────
    # Sanity checks
    # ─────────────────────────────────────────────────────────────────────────
    count = con.execute("SELECT COUNT(*) FROM mart").fetchone()[0]
    span  = con.execute("SELECT MIN(year), MAX(year) FROM mart").fetchone()
    rgns  = con.execute("SELECT DISTINCT rgn FROM mart ORDER BY rgn").fetchall()
    logger.info(f"mart: {count} rows, years {span[0]}–{span[1]}")
    logger.info(f"Regions: {[r[0] for r in rgns]}")

    flag_counts = con.execute("""
        SELECT is_projected, is_interpolated, COUNT(*) AS n
        FROM mart GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    for proj, interp, n in flag_counts:
        tag = "projected" if proj else ("interpolated" if interp else "actual")
        logger.info(f"  {tag:14s}: {n:3d} rows")

    sample = con.execute("""
        SELECT year, rgn, ROUND(median_price) AS med_p, ROUND(median_income) AS med_i,
               ROUND(price_to_income_ratio, 1) AS pti, is_interpolated, is_projected
        FROM mart WHERE rgn = 'E12000007' ORDER BY year DESC LIMIT 8
    """).fetchall()
    logger.info(f"London sample (newest first):\n{sample}")

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION D: Write partitioned Parquet
    # ─────────────────────────────────────────────────────────────────────────
    con.execute(f"""
        COPY (SELECT * FROM mart ORDER BY year)
        TO '{MART_PRICE_INCOME}'
        (
            FORMAT PARQUET,
            PARTITION_BY (year),
            CODEC 'ZSTD',
            COMPRESSION_LEVEL 6
        )
    """)
    logger.success(f"mart_price_income written to {MART_PRICE_INCOME} ({count} rows)")


if __name__ == "__main__":
    main()
