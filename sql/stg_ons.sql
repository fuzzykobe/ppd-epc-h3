-- Staging: ONS Postcode Directory → stg_ons.parquet
COPY (
  SELECT
    regexp_replace(upper(trim(pcds)), '\s+', ' ') AS postcode,
    TRY_CAST(lat AS DOUBLE) AS lat,
    TRY_CAST("long" AS DOUBLE) AS lon,
    oa21,
    lsoa21,
    msoa21,
    ladcd,
    ladnm,
    rgn,
    ctry
  FROM read_csv(?, header=true, ignore_errors=true)
  WHERE TRY_CAST(lat AS DOUBLE) IS NOT NULL
    AND TRY_CAST(lat AS DOUBLE) != 0
) TO ?
(FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6);
