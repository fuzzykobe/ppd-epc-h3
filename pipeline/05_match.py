"""Address matching: exact (DuckDB) then fuzzy (rapidfuzz) on unmatched rows."""
from __future__ import annotations

import concurrent.futures
from typing import Any

import click
import duckdb
import polars as pl
from loguru import logger
from rapidfuzz import process, fuzz
from tqdm import tqdm

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS, FUZZY_MAX_WORKERS, FUZZY_THRESHOLD


def _fuzzy_match_postcode(
    args: tuple[str, list[tuple[str, str]], list[tuple[str, str]]]
) -> list[dict[str, Any]]:
    """Match unmatched PPD rows to EPC rows within a single postcode."""
    postcode, ppd_rows, epc_rows = args
    if not ppd_rows or not epc_rows:
        return []

    epc_keys = [r[1] for r in epc_rows]
    epc_lmk = [r[0] for r in epc_rows]
    results = []

    for tid, ppd_key in ppd_rows:
        match = process.extractOne(
            ppd_key, epc_keys, scorer=fuzz.token_sort_ratio, score_cutoff=FUZZY_THRESHOLD
        )
        if match:
            matched_idx = epc_keys.index(match[0])
            results.append({
                "transaction_id": tid,
                "lmk_key": epc_lmk[matched_idx],
                "match_type": "fuzzy",
                "match_score": float(match[1]),
            })

    return results


@click.command()
def main() -> None:
    ppd_path = DATA_STAGED / "stg_ppd.parquet"
    epc_path = DATA_STAGED / "stg_epc.parquet"
    exact_out = DATA_STAGED / "matches_exact.parquet"
    fuzzy_out = DATA_STAGED / "matches_fuzzy.parquet"
    all_out = DATA_STAGED / "matches_all.parquet"

    for p in (ppd_path, epc_path):
        if not p.exists():
            raise FileNotFoundError(f"{p} not found")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    # --- Phase 1: exact match ---
    logger.info("Phase 1: exact match")
    con.execute(f"""
        COPY (
            SELECT
                p.transaction_id,
                e.lmk_key,
                'exact' AS match_type,
                1.0 AS match_score
            FROM '{ppd_path}' p
            JOIN '{epc_path}' e
              ON p.postcode = e.postcode
             AND p.addr_key = e.addr_key
        ) TO '{exact_out}'
        (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6)
    """)
    exact_count = con.execute(f"SELECT COUNT(*) FROM '{exact_out}'").fetchone()[0]
    logger.info(f"Exact matches: {exact_count:,}")

    # --- Phase 2: fuzzy match on unmatched ---
    logger.info("Phase 2: fuzzy match on unmatched PPD rows")

    unmatched_ppd = con.execute(f"""
        SELECT p.transaction_id, p.postcode, p.addr_key
        FROM '{ppd_path}' p
        WHERE NOT EXISTS (
            SELECT 1 FROM '{exact_out}' m WHERE m.transaction_id = p.transaction_id
        )
    """).pl()

    epc_df = con.execute(f"""
        SELECT lmk_key, postcode, addr_key FROM '{epc_path}'
    """).pl()

    # Group by postcode for parallel processing
    ppd_by_pc: dict[str, list[tuple[str, str]]] = {}
    for row in unmatched_ppd.iter_rows():
        tid, pc, key = row
        ppd_by_pc.setdefault(pc, []).append((tid, key))

    epc_by_pc: dict[str, list[tuple[str, str]]] = {}
    for row in epc_df.iter_rows():
        lmk, pc, key = row
        epc_by_pc.setdefault(pc, []).append((lmk, key))

    postcodes = [pc for pc in ppd_by_pc if pc in epc_by_pc]
    args_list = [(pc, ppd_by_pc[pc], epc_by_pc[pc]) for pc in postcodes]

    fuzzy_rows: list[dict[str, Any]] = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=FUZZY_MAX_WORKERS) as pool:
        for batch in tqdm(
            pool.map(_fuzzy_match_postcode, args_list, chunksize=100),
            total=len(args_list),
            desc="fuzzy matching postcodes",
        ):
            fuzzy_rows.extend(batch)

    if fuzzy_rows:
        fuzzy_df = pl.DataFrame(fuzzy_rows)
        fuzzy_df.write_parquet(str(fuzzy_out), compression="zstd", compression_level=6)
        logger.info(f"Fuzzy matches: {len(fuzzy_rows):,}")
    else:
        pl.DataFrame({
            "transaction_id": pl.Series([], dtype=pl.Utf8),
            "lmk_key": pl.Series([], dtype=pl.Utf8),
            "match_type": pl.Series([], dtype=pl.Utf8),
            "match_score": pl.Series([], dtype=pl.Float64),
        }).write_parquet(str(fuzzy_out))
        logger.warning("No fuzzy matches found")

    # --- Phase 3: combine + mark unmatched ---
    logger.info("Phase 3: combining matches")
    con.execute(f"""
        COPY (
            SELECT transaction_id, lmk_key, match_type, match_score
            FROM '{exact_out}'
            UNION ALL
            SELECT transaction_id, lmk_key, match_type, match_score
            FROM '{fuzzy_out}'
            UNION ALL
            SELECT p.transaction_id, NULL AS lmk_key, 'unmatched' AS match_type, NULL AS match_score
            FROM '{ppd_path}' p
            WHERE NOT EXISTS (
                SELECT 1 FROM '{exact_out}' m WHERE m.transaction_id = p.transaction_id
            ) AND NOT EXISTS (
                SELECT 1 FROM '{fuzzy_out}' m WHERE m.transaction_id = p.transaction_id
            )
        ) TO '{all_out}'
        (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6)
    """)

    total = con.execute(f"SELECT COUNT(*) FROM '{all_out}'").fetchone()[0]
    matched = con.execute(f"""
        SELECT COUNT(*) FROM '{all_out}' WHERE match_type != 'unmatched'
    """).fetchone()[0]
    rate = 100.0 * matched / total if total else 0
    logger.success(f"matches_all.parquet written — {total:,} rows, {rate:.1f}% matched")


if __name__ == "__main__":
    main()
