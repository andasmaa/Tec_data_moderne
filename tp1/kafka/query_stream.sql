INSTALL httpfs; LOAD httpfs;
CREATE SECRET minio (
  TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin',
  ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false
);
.timer on

-- Nombre d'événements et de fichiers reçus en streaming
SELECT count(*) AS nb_evenements, count(DISTINCT filename) AS nb_fichiers
FROM read_parquet('s3://bronze/stream/orders/*/*.parquet', filename = true);

-- Doublons éventuels (doit être vide si aucun consommateur n'a été interrompu)
SELECT order_id, count(*) AS nb
FROM read_parquet('s3://bronze/stream/orders/*/*.parquet')
GROUP BY order_id HAVING count(*) > 1;

-- Aperçu
SELECT * FROM read_parquet('s3://bronze/stream/orders/*/*.parquet') LIMIT 5;
