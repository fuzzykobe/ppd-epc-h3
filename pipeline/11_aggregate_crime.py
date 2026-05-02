"""Aggregate H3-indexed crime records into mart_crime and mart_crime_monthly."""
import click
import duckdb
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS, OUTPUTS


@click.command()
def main() -> None:
    src = DATA_STAGED / "crime_h3.parquet"
    mart_crime_dir = OUTPUTS / "mart_crime"
    mart_monthly_dir = OUTPUTS / "mart_crime_monthly"

    if not src.exists():
        raise FileNotFoundError(f"{src} not found — run step 10 first")

    OUTPUTS.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    # Register source so we can reference it twice without re-reading
    con.execute(f"CREATE VIEW crime_h3 AS SELECT * FROM read_parquet('{src}')")

    # ------------------------------------------------------------------ #
    # mart_crime: one row per (h3_r7..r10, year, month_date, crime_type) #
    # ------------------------------------------------------------------ #
    logger.info("Building mart_crime (long format by crime type)")
    con.execute(f"""
        COPY (
            SELECT
                h3_r7,
                h3_r8,
                h3_r9,
                h3_r10,
                year,
                month                                       AS month_date,
                crime_type,
                COUNT(*)                                    AS crime_count,
                -- mode: most frequent force and lsoa within group
                mode(force)                                 AS force,
                mode(lsoa_code)                             AS lsoa_code
            FROM crime_h3
            GROUP BY h3_r7, h3_r8, h3_r9, h3_r10, year, month, crime_type
            ORDER BY year, month
        ) TO '{mart_crime_dir}'
        (
            FORMAT PARQUET,
            PARTITION_BY (year),
            CODEC 'ZSTD',
            COMPRESSION_LEVEL 6,
            ROW_GROUP_SIZE 500000
        )
    """)
    long_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{mart_crime_dir}/**/*.parquet')
    """).fetchone()[0]
    logger.info(f"mart_crime: {long_count:,} rows")

    # ------------------------------------------------------------------ #
    # mart_crime_monthly: pivoted wide — one column per crime type        #
    # ------------------------------------------------------------------ #
    logger.info("Building mart_crime_monthly (wide pivot by crime type)")

    # Get distinct crime types to build dynamic pivot SQL
    types = [
        r[0] for r in con.execute(
            "SELECT DISTINCT crime_type FROM crime_h3 ORDER BY crime_type"
        ).fetchall()
    ]

    pivot_cols = ",\n            ".join(
        f"COUNT(*) FILTER (WHERE crime_type = '{t}') "
        f"AS \"{t.lower().replace(' ', '_').replace('-', '_').replace('/', '_')}\""
        for t in types
    )

    con.execute(f"""
        COPY (
            SELECT
                h3_r7,
                h3_r8,
                h3_r9,
                h3_r10,
                year,
                month                                       AS month_date,
                COUNT(*)                                    AS total_crimes,
                {pivot_cols}
            FROM crime_h3
            GROUP BY h3_r7, h3_r8, h3_r9, h3_r10, year, month
            ORDER BY year, month
        ) TO '{mart_monthly_dir}'
        (
            FORMAT PARQUET,
            PARTITION_BY (year),
            CODEC 'ZSTD',
            COMPRESSION_LEVEL 6,
            ROW_GROUP_SIZE 500000
        )
    """)
    wide_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{mart_monthly_dir}/**/*.parquet')
    """).fetchone()[0]
    logger.info(f"mart_crime_monthly: {wide_count:,} rows")

    # ------------------------------------------------------------------ #
    # Summary report                                                       #
    # ------------------------------------------------------------------ #
    summary = con.execute("""
        SELECT
            COUNT(*)                            AS total_crimes,
            MIN(month)                          AS earliest,
            MAX(month)                          AS latest,
            COUNT(DISTINCT h3_r7)               AS cells_r7,
            COUNT(DISTINCT h3_r8)               AS cells_r8,
            COUNT(DISTINCT h3_r9)               AS cells_r9,
            COUNT(DISTINCT h3_r10)              AS cells_r10
        FROM crime_h3
    """).fetchone()

    logger.success(
        f"Crime aggregation complete — {summary[0]:,} records · "
        f"{summary[1]} → {summary[2]}"
    )
    logger.info(f"Distinct H3 cells: r7={summary[3]:,}  r8={summary[4]:,}  "
                f"r9={summary[5]:,}  r10={summary[6]:,}")

    top10 = con.execute("""
        SELECT crime_type, COUNT(*) AS n
        FROM crime_h3 GROUP BY crime_type ORDER BY n DESC LIMIT 10
    """).fetchall()
    logger.info("Top 10 crime types:")
    for t in top10:
        logger.info(f"  {t[0]:45s} {t[1]:>9,}")

    return summary  # consumed by 08_build_mart when updating match_report


if __name__ == "__main__":
    main()
