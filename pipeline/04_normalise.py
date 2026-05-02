"""
Verify addr_key columns exist and spot-check normalisation quality.
addr_key is computed inline during ingest (steps 1 & 2), so this step
is a validation + optional recompute if staging files need to be refreshed.
"""
import click
import duckdb
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS


@click.command()
def main() -> None:
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    for name in ("stg_ppd", "stg_epc"):
        path = DATA_STAGED / f"{name}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"{path} not found — run ingest steps first")

        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()]
        if "addr_key" not in cols:
            raise RuntimeError(f"addr_key missing from {name} — re-run ingest step")

        null_pct = con.execute(f"""
            SELECT 100.0 * COUNT(*) FILTER (WHERE addr_key IS NULL OR addr_key = '')
                / COUNT(*) FROM '{path}'
        """).fetchone()[0]
        sample = con.execute(f"""
            SELECT postcode, addr_key FROM '{path}' LIMIT 5
        """).fetchall()

        logger.info(f"{name}: addr_key null/empty = {null_pct:.2f}%")
        for row in sample:
            logger.debug(f"  {row[0]!r:12s} | {row[1]!r}")

    logger.success("Normalisation check passed")


if __name__ == "__main__":
    main()
