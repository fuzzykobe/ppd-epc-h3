-- Exact address match: join PPD and EPC within postcode blocks on normalised addr_key
SELECT
    p.transaction_id,
    e.lmk_key,
    'exact' AS match_type,
    1.0 AS match_score
FROM read_parquet(?) p
JOIN read_parquet(?) e
  ON p.postcode = e.postcode
 AND p.addr_key  = e.addr_key;
