INSTALL httpfs; LOAD httpfs;
CREATE SECRET minio (
  TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin',
  ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false
);
.timer on

-- Requête 1 : CSV
SELECT city, count(*) AS nb, round(sum(amount), 2) AS ca
FROM read_csv_auto('s3://bronze/csv/orders/*/*.csv')
WHERE status = 'PAID'
GROUP BY city ORDER BY ca DESC LIMIT 10;

-- Requête 2 : Parquet
SELECT city, count(*) AS nb, round(sum(amount), 2) AS ca
FROM read_parquet('s3://bronze/parquet/orders/*/*.parquet', hive_partitioning = true)
WHERE status = 'PAID'
GROUP BY city ORDER BY ca DESC LIMIT 10;

-- Requête 3 : colonne de partition
SELECT ingestion_date, count(*)
FROM read_parquet('s3://bronze/parquet/orders/*/*.parquet', hive_partitioning = true)
GROUP BY ALL;

-- Requête 4 : plan d'exécution
EXPLAIN ANALYZE
SELECT sum(amount) FROM read_parquet('s3://bronze/parquet/orders/*/*.parquet');

-- Requête 5 : métadonnées Parquet
SELECT row_group_id, path_in_schema, stats_min, stats_max, compression
FROM parquet_metadata('s3://bronze/parquet/orders/*/*.parquet')
WHERE path_in_schema = 'amount';
