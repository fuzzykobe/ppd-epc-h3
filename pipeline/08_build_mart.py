"""Write final mart partitioned by transfer_year → outputs/mart_transactions/"""
import click
import duckdb
import json
from datetime import datetime, timezone
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS, OUTPUTS


@click.command()
def main() -> None:
    src = DATA_STAGED / "transactions_h3.parquet"
    mart_dir = OUTPUTS / "mart_transactions"
    report_path = OUTPUTS / "match_report.json"

    if not src.exists():
        raise FileNotFoundError(f"{src} not found")

    OUTPUTS.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    # Use 24GB here: full-dataset ORDER BY postcode OOMs at 40GB due to sort
    # spill overhead. Partition write doesn't need full-sort; ORDER BY year only.
    con.execute("PRAGMA memory_limit='24GB'")

    logger.info(f"Writing partitioned mart to {mart_dir}")
    con.execute(f"""
        COPY (
            SELECT * FROM '{src}'
            ORDER BY transfer_year
        ) TO '{mart_dir}'
        (
            FORMAT PARQUET,
            PARTITION_BY (transfer_year),
            CODEC 'ZSTD',
            COMPRESSION_LEVEL 6,
            ROW_GROUP_SIZE 500000
        )
    """)

    # --- Match report ---
    stats = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE match_type = 'exact') AS exact,
            COUNT(*) FILTER (WHERE match_type = 'fuzzy') AS fuzzy,
            COUNT(*) FILTER (WHERE match_type = 'uprn') AS uprn,
            COUNT(*) FILTER (WHERE match_type = 'unmatched') AS unmatched,
            COUNT(*) FILTER (WHERE lat IS NOT NULL) AS geo_enriched,
            COUNT(*) FILTER (WHERE h3_r7 IS NOT NULL) AS h3_applied
        FROM '{src}'
    """).fetchone()

    total, exact, fuzzy, uprn, unmatched, geo, h3_applied = stats
    matched = exact + fuzzy + uprn
    report = {
        "ppd_category_a": total,
        "matched_exact": exact,
        "matched_fuzzy": fuzzy,
        "matched_uprn": uprn,
        "unmatched": unmatched,
        "match_rate_pct": round(100.0 * matched / total, 2) if total else 0,
        "geo_enriched_pct": round(100.0 * geo / total, 2) if total else 0,
        "h3_applied_pct": round(100.0 * h3_applied / total, 2) if total else 0,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    report_path.write_text(json.dumps(report, indent=2))

    logger.success(f"Mart written. Match rate: {report['match_rate_pct']}%")
    logger.info(f"Match report: {report_path}")


if __name__ == "__main__":
    main()
