-- Fuzzy match candidates: unmatched PPD rows joined to all EPC rows in same postcode.
-- Actual token_sort_ratio scoring is done in Python (rapidfuzz); this query
-- extracts the unmatched PPD rows for Python to process.
SELECT
    p.transaction_id,
    p.postcode,
    p.addr_key
FROM read_parquet(?) p  -- stg_ppd
WHERE NOT EXISTS (
    SELECT 1
    FROM read_parquet(?) m  -- matches_exact
    WHERE m.transaction_id = p.transaction_id
);
