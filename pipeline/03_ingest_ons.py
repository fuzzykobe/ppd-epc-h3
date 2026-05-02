"""Ingest ONS Postcode Directory → data/staged/stg_ons.parquet"""
import click
import duckdb
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS, ONS_CSV


@click.command()
def main() -> None:
    if not ONS_CSV.exists():
        raise FileNotFoundError(f"ONSPD CSV not found: {ONS_CSV}")

    DATA_STAGED.mkdir(parents=True, exist_ok=True)
    out = DATA_STAGED / "stg_ons.parquet"

    logger.info(f"Reading ONSPD from {ONS_CSV}")
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    con.execute(f"""
        COPY (
            SELECT
                regexp_replace(upper(trim(pcds)), '\\s+', ' ') AS postcode,
                TRY_CAST(lat AS DOUBLE) AS lat,
                TRY_CAST("long" AS DOUBLE) AS lon,
                oa21,
                lsoa21,
                msoa21,
                ladcd,
                ladnm,
                rgn,
                ctry
            FROM read_csv(
                '{ONS_CSV}',
                header=true,
                ignore_errors=true
            )
            WHERE TRY_CAST(lat AS DOUBLE) IS NOT NULL
              AND TRY_CAST(lat AS DOUBLE) != 0
        ) TO '{out}'
        (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6)
    """)

    count = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    logger.success(f"stg_ons.parquet written — {count:,} rows")


if __name__ == "__main__":
    main()
