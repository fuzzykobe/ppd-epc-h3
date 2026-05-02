"""Ingest police street-level crime CSVs → data/staged/stg_crime.parquet"""
import glob as glob_module

import click
import duckdb
from loguru import logger

from pipeline.config import (
    CRIME_DIR,
    CRIME_KEEP_TYPES,
    DATA_STAGED,
    DUCKDB_MEMORY,
    DUCKDB_THREADS,
)


@click.command()
def main() -> None:
    pattern = str(CRIME_DIR / "**" / "*-street.csv")
    files = glob_module.glob(pattern, recursive=True)
    if not files:
        raise FileNotFoundError(
            f"No *-street.csv files found under {CRIME_DIR}. "
            "Extract latest.zip preserving folder structure first."
        )

    DATA_STAGED.mkdir(parents=True, exist_ok=True)
    out = DATA_STAGED / "stg_crime.parquet"

    logger.info(f"Found {len(files):,} *-street.csv files across all forces/months")

    crime_type_filter = ""
    if CRIME_KEEP_TYPES:
        types_sql = ", ".join(f"'{t}'" for t in CRIME_KEEP_TYPES)
        crime_type_filter = f"WHERE crime_type IN ({types_sql})"

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    con.execute(f"""
        COPY (
            SELECT
                "Crime ID"                                          AS crime_id,
                TRY_CAST(
                    strptime(trim("Month"), '%Y-%m') AS DATE
                )                                                   AS month,
                YEAR(TRY_CAST(
                    strptime(trim("Month"), '%Y-%m') AS DATE
                ))                                                  AS year,
                trim("Reported by")                                 AS force,
                TRY_CAST("Longitude" AS DOUBLE)                    AS longitude,
                TRY_CAST("Latitude"  AS DOUBLE)                    AS latitude,
                trim("Location")                                    AS location,
                trim("LSOA code")                                   AS lsoa_code,
                trim("LSOA name")                                   AS lsoa_name,
                trim("Crime type")                                  AS crime_type,
                trim("Last outcome category")                       AS last_outcome,
                trim("Context")                                     AS context
            FROM read_csv(
                '{CRIME_DIR}/**/*-street.csv',
                header=true,
                union_by_name=true,
                ignore_errors=true
            )
            WHERE TRY_CAST("Latitude"  AS DOUBLE) IS NOT NULL
              AND TRY_CAST("Longitude" AS DOUBLE) IS NOT NULL
            {crime_type_filter}
        ) TO '{out}'
        (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6)
    """)

    count = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    date_range = con.execute(f"""
        SELECT MIN(month), MAX(month) FROM '{out}'
    """).fetchone()

    logger.success(f"stg_crime.parquet written — {count:,} rows")
    logger.info(f"Date range: {date_range[0]} → {date_range[1]}")

    # Crime type breakdown
    logger.info("Crime type breakdown (top 15):")
    types = con.execute(f"""
        SELECT crime_type, COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / {count}, 2) AS pct
        FROM '{out}'
        GROUP BY crime_type ORDER BY n DESC LIMIT 15
    """).fetchall()
    for t in types:
        logger.info(f"  {t[0]:45s} {t[1]:>8,}  ({t[2]:.1f}%)")


if __name__ == "__main__":
    main()
