-- Final mart: write transactions_h3 partitioned by transfer_year
COPY (
    SELECT * FROM read_parquet(?)
    ORDER BY transfer_year, postcode
) TO ?
(
    FORMAT PARQUET,
    PARTITION_BY (transfer_year),
    CODEC 'ZSTD',
    COMPRESSION_LEVEL 6,
    ROW_GROUP_SIZE 500000
);
