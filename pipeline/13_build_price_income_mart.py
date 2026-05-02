"""
Build price-to-income mart.

Joins mart_transactions to stg_income on msoa21 × financial_year, then
aggregates to annual regional level:

    year, rgn, region_name,
    median_price, median_income, price_to_income_ratio,
    ftb_proxy_price (lower quartile of flats+terraced),
    ftb_price_to_income

Writes outputs/mart_price_income/ partitioned by year, ZSTD.
"""
import click
import duckdb
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS, OUTPUTS

INCOME_PARQUET   = DATA_STAGED / "stg_income.parquet"
MART_TRANSACTIONS = OUTPUTS / "mart_transactions"
MART_PRICE_INCOME = OUTPUTS / "mart_price_income"

RGN_NAMES = {
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
}


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

    # ── Create views ──────────────────────────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE VIEW transactions AS
        SELECT * FROM read_parquet('{MART_TRANSACTIONS}/**/*.parquet',
            hive_partitioning=true)
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW income AS
        SELECT * FROM read_parquet('{INCOME_PARQUET}')
    """)

    # ── Diagnostic: check coverage ─────────────────────────────────────────
    tx_msoa = con.execute("SELECT COUNT(DISTINCT msoa21) FROM transactions WHERE msoa21 IS NOT NULL").fetchone()[0]
    inc_msoa = con.execute("SELECT COUNT(DISTINCT msoa_code) FROM income").fetchone()[0]
    inc_years = con.execute("SELECT MIN(financial_year), MAX(financial_year) FROM income").fetchone()
    logger.info(f"Transactions: {tx_msoa:,} distinct MSOAs")
    logger.info(f"Income: {inc_msoa:,} distinct MSOAs, years {inc_years[0]}–{inc_years[1]}")

    match_count = con.execute("""
        SELECT COUNT(DISTINCT t.msoa21) FROM transactions t
        JOIN income i ON t.msoa21 = i.msoa_code
    """).fetchone()[0]
    logger.info(f"MSOA join coverage: {match_count:,} of {tx_msoa:,} transaction MSOAs matched")

    # ── Build regional aggregates ──────────────────────────────────────────
    logger.info("Building regional price-to-income aggregates…")

    # Financial year alignment: financial_year 2021 = year "2021-22" covers Apr 2021–Mar 2022.
    # We match transfer_year to financial_year (e.g. transfer_year=2021 → financial_year=2021).
    # DuckDB 1.x doesn't support FILTER on ordered-set aggregates (PERCENTILE_CONT).
    # Workaround: separate FTB CTE that pre-filters to flats+terraced.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE mart AS
        WITH tx_income AS (
            SELECT
                t.transfer_year                  AS year,
                t.rgn,
                t.price,
                t.property_type,
                i.net_income_median              AS msoa_income_median
            FROM transactions t
            JOIN income i
                ON t.msoa21 = i.msoa_code
               AND t.transfer_year = i.financial_year
            WHERE t.rgn IS NOT NULL
              AND t.price > 10000
              AND t.rgn NOT LIKE 'S%'
              AND i.net_income_median > 1000
        ),
        all_props AS (
            SELECT year, rgn,
                MEDIAN(price)::DOUBLE                AS median_price,
                MEDIAN(msoa_income_median)::DOUBLE   AS median_income,
                COUNT(*)                             AS n_transactions
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
            a.median_price / NULLIF(a.median_income, 0)                   AS price_to_income_ratio,
            f.ftb_proxy_price,
            f.ftb_proxy_price / NULLIF(a.median_income, 0)                AS ftb_price_to_income,
            a.n_transactions
        FROM all_props a
        LEFT JOIN ftb_props f ON a.year = f.year AND a.rgn = f.rgn
        WHERE a.year BETWEEN 2002 AND 2025
          AND a.median_price IS NOT NULL
          AND a.median_income IS NOT NULL
        ORDER BY a.year, a.rgn
    """)

    # ── National (excl London) aggregates ──────────────────────────────────
    con.execute("""
        INSERT INTO mart
        WITH tx_income AS (
            SELECT
                t.transfer_year                  AS year,
                t.price,
                t.property_type,
                i.net_income_median              AS msoa_income_median
            FROM transactions t
            JOIN income i
                ON t.msoa21 = i.msoa_code
               AND t.transfer_year = i.financial_year
            WHERE t.rgn IS NOT NULL
              AND t.rgn NOT IN ('E12000007', 'S99999999')
              AND t.price > 10000
              AND i.net_income_median > 1000
        ),
        all_props AS (
            SELECT year,
                MEDIAN(price)::DOUBLE               AS median_price,
                MEDIAN(msoa_income_median)::DOUBLE  AS median_income,
                COUNT(*)                            AS n_transactions
            FROM tx_income
            WHERE year BETWEEN 2002 AND 2025
            GROUP BY year
        ),
        ftb_props AS (
            SELECT year,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)::DOUBLE AS ftb_proxy_price
            FROM tx_income
            WHERE property_type IN ('F', 'T')
              AND year BETWEEN 2002 AND 2025
            GROUP BY year
        )
        SELECT
            a.year,
            'excl_london'                                                   AS rgn,
            a.median_price,
            a.median_income,
            a.median_price / NULLIF(a.median_income, 0)                    AS price_to_income_ratio,
            f.ftb_proxy_price,
            f.ftb_proxy_price / NULLIF(a.median_income, 0)                 AS ftb_price_to_income,
            a.n_transactions
        FROM all_props a
        LEFT JOIN ftb_props f ON a.year = f.year
        WHERE a.median_price IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM mart").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM mart").fetchone()
    rgns  = con.execute("SELECT DISTINCT rgn FROM mart ORDER BY rgn").fetchall()
    logger.info(f"mart: {count} rows, years {years[0]}–{years[1]}")
    logger.info(f"regions: {[r[0] for r in rgns]}")

    # Sanity check
    sample = con.execute("""
        SELECT year, rgn, ROUND(median_price) AS med_p, ROUND(median_income) AS med_i,
               ROUND(price_to_income_ratio,1) AS pti
        FROM mart WHERE rgn = 'E12000007' ORDER BY year DESC LIMIT 5
    """).fetchall()
    logger.info(f"London sample: {sample}")

    # ── Write partitioned parquet ──────────────────────────────────────────
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
