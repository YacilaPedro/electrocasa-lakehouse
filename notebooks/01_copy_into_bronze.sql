-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · COPY INTO → bronze (catalogo y sucursales)
-- MAGIC Carga incremental e idempotente: `COPY INTO` omite los archivos ya cargados, por lo que el job puede reintentarse sin duplicar.
-- MAGIC Sin logica de negocio: todo STRING + columnas tecnicas `_ingested_at`, `_source_file`, `_batch_id`.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'electrocasa_dev';

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE v_catalog STRING;
SET VAR v_catalog = :catalog;

-- COMMAND ----------

-- Tablas destino con esquema explicito (todo STRING; el tipado ocurre en Silver)
CREATE TABLE IF NOT EXISTS IDENTIFIER(v_catalog || '.bronze.catalogo') (
  producto_id STRING,
  nombre_producto STRING,
  categoria STRING,
  marca STRING,
  precio_lista STRING,
  activo STRING,
  _ingested_at TIMESTAMP,
  _source_file STRING,
  _batch_id STRING
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(v_catalog || '.bronze.sucursales') (
  sucursal_id STRING,
  nombre_sucursal STRING,
  distrito STRING,
  ciudad STRING,
  region STRING,
  fecha_apertura STRING,
  _ingested_at TIMESTAMP,
  _source_file STRING,
  _batch_id STRING
);

-- COMMAND ----------

-- catalogo: JSON multilinea, 2 snapshots completos (marzo y agosto)
EXECUTE IMMEDIATE concat(
  'COPY INTO ', v_catalog, '.bronze.catalogo FROM (',
  'SELECT producto_id, nombre_producto, categoria, marca, precio_lista, activo, ',
  'current_timestamp() AS _ingested_at, ',
  '_metadata.file_name AS _source_file, ',
  'coalesce(nullif(regexp_extract(_metadata.file_name, \'([0-9]{4}-[0-9]{2}-[0-9]{2})\', 1), \'\'), date_format(_metadata.file_modification_time, \'yyyy-MM-dd\')) AS _batch_id ',
  'FROM \'/Volumes/', v_catalog, '/default/vol_landing/catalogo/\') ',
  'FILEFORMAT = JSON ',
  'FORMAT_OPTIONS (\'multiLine\' = \'true\', \'primitivesAsString\' = \'true\')'
);

-- COMMAND ----------

-- sucursales: CSV unico de 40 filas (sin inferSchema: todo STRING)
EXECUTE IMMEDIATE concat(
  'COPY INTO ', v_catalog, '.bronze.sucursales FROM (',
  'SELECT sucursal_id, nombre_sucursal, distrito, ciudad, region, fecha_apertura, ',
  'current_timestamp() AS _ingested_at, ',
  '_metadata.file_name AS _source_file, ',
  'coalesce(nullif(regexp_extract(_metadata.file_name, \'([0-9]{4}-[0-9]{2}-[0-9]{2})\', 1), \'\'), date_format(_metadata.file_modification_time, \'yyyy-MM-dd\')) AS _batch_id ',
  'FROM \'/Volumes/', v_catalog, '/default/vol_landing/sucursales/\') ',
  'FILEFORMAT = CSV ',
  'FORMAT_OPTIONS (\'header\' = \'true\', \'inferSchema\' = \'false\')'
);

-- COMMAND ----------

-- Conteos por lote (snapshot / archivo) para verificar la carga
SELECT 'catalogo' AS fuente, _batch_id, _source_file, count(*) AS filas
FROM IDENTIFIER(v_catalog || '.bronze.catalogo') GROUP BY _batch_id, _source_file
UNION ALL
SELECT 'sucursales', _batch_id, _source_file, count(*)
FROM IDENTIFIER(v_catalog || '.bronze.sucursales') GROUP BY _batch_id, _source_file
ORDER BY fuente, _batch_id;
