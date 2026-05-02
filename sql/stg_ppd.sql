-- Staging: PPD raw CSV → stg_ppd.parquet
-- Category A only, exclude deleted records, normalise addresses
COPY (
  SELECT
    transaction_id,
    CAST(price AS INTEGER) AS price,
    CAST(transfer_date AS DATE) AS transfer_date,
    YEAR(CAST(transfer_date AS DATE)) AS transfer_year,
    MONTH(CAST(transfer_date AS DATE)) AS transfer_month,
    regexp_replace(upper(trim(postcode)), '\s+', ' ') AS postcode,
    property_type,
    old_new,
    duration,
    upper(trim(paon)) AS paon,
    NULLIF(upper(trim(saon)), '') AS saon,
    upper(trim(street)) AS street,
    upper(trim(town_city)) AS town_city,
    upper(trim(district)) AS district,
    upper(trim(county)) AS county,
    ppd_category,
    regexp_replace(
      regexp_replace(
        upper(trim(concat_ws(' ', NULLIF(upper(trim(saon)), ''), upper(trim(paon)), upper(trim(street))))),
        '[^A-Z0-9 ]', ' ', 'g'
      ),
      '\s+', ' ', 'g'
    ) AS addr_key
  FROM read_csv(
    ?,
    header=false,
    columns={
      'transaction_id': 'VARCHAR', 'price': 'VARCHAR', 'transfer_date': 'VARCHAR',
      'postcode': 'VARCHAR', 'property_type': 'VARCHAR', 'old_new': 'VARCHAR',
      'duration': 'VARCHAR', 'paon': 'VARCHAR', 'saon': 'VARCHAR',
      'street': 'VARCHAR', 'locality': 'VARCHAR', 'town_city': 'VARCHAR',
      'district': 'VARCHAR', 'county': 'VARCHAR',
      'ppd_category': 'VARCHAR', 'record_status': 'VARCHAR'
    }
  )
  WHERE record_status != 'D'
  AND ppd_category = 'A'
) TO ?
(FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6);
