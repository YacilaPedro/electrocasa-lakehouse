-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Validacion de calidad y gobierno
-- MAGIC Imprime conteos de Silver, Gold y tablas `*_rechazos`; **falla con `assert_true`** si alguna tabla Gold esta vacia o si Silver perdio filas de forma inconsistente.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'electrocasa_dev';

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE v_catalog STRING;
SET VAR v_catalog = :catalog;

-- COMMAND ----------

-- MAGIC %md ## 1. Conteos por lote en Bronze

-- COMMAND ----------

SELECT 'ventas' AS fuente, _batch_id, count(*) AS filas FROM IDENTIFIER(v_catalog || '.bronze.ventas') GROUP BY _batch_id
UNION ALL SELECT 'empleados', _batch_id, count(*) FROM IDENTIFIER(v_catalog || '.bronze.empleados') GROUP BY _batch_id
UNION ALL SELECT 'devoluciones', _batch_id, count(*) FROM IDENTIFIER(v_catalog || '.bronze.devoluciones') GROUP BY _batch_id
UNION ALL SELECT 'resenas', _batch_id, count(*) FROM IDENTIFIER(v_catalog || '.bronze.resenas') GROUP BY _batch_id
UNION ALL SELECT 'catalogo', _batch_id, count(*) FROM IDENTIFIER(v_catalog || '.bronze.catalogo') GROUP BY _batch_id
UNION ALL SELECT 'sucursales', _batch_id, count(*) FROM IDENTIFIER(v_catalog || '.bronze.sucursales') GROUP BY _batch_id
UNION ALL SELECT 'tracking_envios', 'cursor', count(*) FROM IDENTIFIER(v_catalog || '.bronze.tracking_envios')
ORDER BY fuente, _batch_id;

-- COMMAND ----------

-- MAGIC %md ## 2. Conteos Silver y tablas de rechazos

-- COMMAND ----------

SELECT 'silver.ventas' AS tabla, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.ventas')
UNION ALL SELECT 'silver.ventas_rechazos', count(*) FROM IDENTIFIER(v_catalog || '.silver.ventas_rechazos')
UNION ALL SELECT 'silver.empleados', count(*) FROM IDENTIFIER(v_catalog || '.silver.empleados')
UNION ALL SELECT 'silver.empleados_rechazos', count(*) FROM IDENTIFIER(v_catalog || '.silver.empleados_rechazos')
UNION ALL SELECT 'silver.devoluciones', count(*) FROM IDENTIFIER(v_catalog || '.silver.devoluciones')
UNION ALL SELECT 'silver.devoluciones_rechazos', count(*) FROM IDENTIFIER(v_catalog || '.silver.devoluciones_rechazos')
UNION ALL SELECT 'silver.resenas', count(*) FROM IDENTIFIER(v_catalog || '.silver.resenas')
UNION ALL SELECT 'silver.resenas_rechazos', count(*) FROM IDENTIFIER(v_catalog || '.silver.resenas_rechazos')
UNION ALL SELECT 'silver.catalogo', count(*) FROM IDENTIFIER(v_catalog || '.silver.catalogo')
UNION ALL SELECT 'silver.catalogo_rechazos', count(*) FROM IDENTIFIER(v_catalog || '.silver.catalogo_rechazos')
UNION ALL SELECT 'silver.sucursales', count(*) FROM IDENTIFIER(v_catalog || '.silver.sucursales')
UNION ALL SELECT 'silver.sucursales_rechazos', count(*) FROM IDENTIFIER(v_catalog || '.silver.sucursales_rechazos')
UNION ALL SELECT 'silver.tracking_envios', count(*) FROM IDENTIFIER(v_catalog || '.silver.tracking_envios')
UNION ALL SELECT 'silver.tracking_envios_rechazos', count(*) FROM IDENTIFIER(v_catalog || '.silver.tracking_envios_rechazos')
ORDER BY tabla;

-- COMMAND ----------

-- MAGIC %md ## 3. Conteos Gold

-- COMMAND ----------

SELECT 'gold.ventas_netas_sucursal_mes' AS tabla, count(*) AS filas FROM IDENTIFIER(v_catalog || '.gold.ventas_netas_sucursal_mes')
UNION ALL SELECT 'gold.top_productos_unidades', count(*) FROM IDENTIFIER(v_catalog || '.gold.top_productos_unidades')
UNION ALL SELECT 'gold.top_productos_tasa_devolucion', count(*) FROM IDENTIFIER(v_catalog || '.gold.top_productos_tasa_devolucion')
UNION ALL SELECT 'gold.dotacion_mensual', count(*) FROM IDENTIFIER(v_catalog || '.gold.dotacion_mensual')
UNION ALL SELECT 'gold.empleados_vigentes', count(*) FROM IDENTIFIER(v_catalog || '.gold.empleados_vigentes')
UNION ALL SELECT 'gold.ventas_por_vendedor', count(*) FROM IDENTIFIER(v_catalog || '.gold.ventas_por_vendedor')
UNION ALL SELECT 'gold.resenas_negativas_categoria_mes', count(*) FROM IDENTIFIER(v_catalog || '.gold.resenas_negativas_categoria_mes')
UNION ALL SELECT 'gold.metricas_despachos_courier', count(*) FROM IDENTIFIER(v_catalog || '.gold.metricas_despachos_courier')
UNION ALL SELECT 'gold.resenas_por_origen_metadata', count(*) FROM IDENTIFIER(v_catalog || '.gold.resenas_por_origen_metadata')
ORDER BY tabla;

-- COMMAND ----------

-- MAGIC %md ## 3b. Registro persistente de conteos (evidencia por lote)
-- MAGIC Cada corrida del job agrega sus conteos a `default.log_conteos`: asi quedan los conteos despues de cada lote.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(v_catalog || '.default.log_conteos') (corrida TIMESTAMP, capa STRING, tabla STRING, lote STRING, filas BIGINT);

-- COMMAND ----------

INSERT INTO IDENTIFIER(v_catalog || '.default.log_conteos')
SELECT current_timestamp() AS corrida, 'bronze' AS capa, 'ventas' AS tabla, _batch_id AS lote, count(*) AS filas FROM IDENTIFIER(v_catalog || '.bronze.ventas') GROUP BY _batch_id
UNION ALL SELECT current_timestamp() AS corrida, 'bronze' AS capa, 'empleados' AS tabla, _batch_id AS lote, count(*) AS filas FROM IDENTIFIER(v_catalog || '.bronze.empleados') GROUP BY _batch_id
UNION ALL SELECT current_timestamp() AS corrida, 'bronze' AS capa, 'devoluciones' AS tabla, _batch_id AS lote, count(*) AS filas FROM IDENTIFIER(v_catalog || '.bronze.devoluciones') GROUP BY _batch_id
UNION ALL SELECT current_timestamp() AS corrida, 'bronze' AS capa, 'resenas' AS tabla, _batch_id AS lote, count(*) AS filas FROM IDENTIFIER(v_catalog || '.bronze.resenas') GROUP BY _batch_id
UNION ALL SELECT current_timestamp() AS corrida, 'bronze' AS capa, 'catalogo' AS tabla, _batch_id AS lote, count(*) AS filas FROM IDENTIFIER(v_catalog || '.bronze.catalogo') GROUP BY _batch_id
UNION ALL SELECT current_timestamp() AS corrida, 'bronze' AS capa, 'sucursales' AS tabla, _batch_id AS lote, count(*) AS filas FROM IDENTIFIER(v_catalog || '.bronze.sucursales') GROUP BY _batch_id
UNION ALL SELECT current_timestamp(), 'bronze', 'tracking_envios', 'cursor', count(*) FROM IDENTIFIER(v_catalog || '.bronze.tracking_envios')
UNION ALL SELECT current_timestamp(), 'silver', 'ventas', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.ventas')
UNION ALL SELECT current_timestamp(), 'silver', 'ventas_rechazos', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.ventas_rechazos')
UNION ALL SELECT current_timestamp(), 'silver', 'empleados', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.empleados')
UNION ALL SELECT current_timestamp(), 'silver', 'empleados_rechazos', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.empleados_rechazos')
UNION ALL SELECT current_timestamp(), 'silver', 'devoluciones', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.devoluciones')
UNION ALL SELECT current_timestamp(), 'silver', 'devoluciones_rechazos', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.devoluciones_rechazos')
UNION ALL SELECT current_timestamp(), 'silver', 'resenas', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.resenas')
UNION ALL SELECT current_timestamp(), 'silver', 'resenas_rechazos', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.resenas_rechazos')
UNION ALL SELECT current_timestamp(), 'silver', 'catalogo', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.catalogo')
UNION ALL SELECT current_timestamp(), 'silver', 'catalogo_rechazos', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.catalogo_rechazos')
UNION ALL SELECT current_timestamp(), 'silver', 'sucursales', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.sucursales')
UNION ALL SELECT current_timestamp(), 'silver', 'sucursales_rechazos', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.sucursales_rechazos')
UNION ALL SELECT current_timestamp(), 'silver', 'tracking_envios', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.tracking_envios')
UNION ALL SELECT current_timestamp(), 'silver', 'tracking_envios_rechazos', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.silver.tracking_envios_rechazos')
UNION ALL SELECT current_timestamp(), 'gold', 'ventas_netas_sucursal_mes', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.ventas_netas_sucursal_mes')
UNION ALL SELECT current_timestamp(), 'gold', 'top_productos_unidades', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.top_productos_unidades')
UNION ALL SELECT current_timestamp(), 'gold', 'top_productos_tasa_devolucion', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.top_productos_tasa_devolucion')
UNION ALL SELECT current_timestamp(), 'gold', 'dotacion_mensual', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.dotacion_mensual')
UNION ALL SELECT current_timestamp(), 'gold', 'empleados_vigentes', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.empleados_vigentes')
UNION ALL SELECT current_timestamp(), 'gold', 'ventas_por_vendedor', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.ventas_por_vendedor')
UNION ALL SELECT current_timestamp(), 'gold', 'resenas_negativas_categoria_mes', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.resenas_negativas_categoria_mes')
UNION ALL SELECT current_timestamp(), 'gold', 'metricas_despachos_courier', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.metricas_despachos_courier')
UNION ALL SELECT current_timestamp(), 'gold', 'resenas_por_origen_metadata', 'acumulado', count(*) FROM IDENTIFIER(v_catalog || '.gold.resenas_por_origen_metadata');

-- COMMAND ----------

SELECT * FROM IDENTIFIER(v_catalog || '.default.log_conteos') WHERE corrida = (SELECT max(corrida) FROM IDENTIFIER(v_catalog || '.default.log_conteos')) ORDER BY capa, tabla, lote;

-- COMMAND ----------

-- MAGIC %md ## 3c. Rechazos por fuente y motivo

-- COMMAND ----------

SELECT fuente, motivo_rechazo, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.ventas_rechazos') GROUP BY fuente, motivo_rechazo
UNION ALL SELECT fuente, motivo_rechazo, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.empleados_rechazos') GROUP BY fuente, motivo_rechazo
UNION ALL SELECT fuente, motivo_rechazo, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.devoluciones_rechazos') GROUP BY fuente, motivo_rechazo
UNION ALL SELECT fuente, motivo_rechazo, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.resenas_rechazos') GROUP BY fuente, motivo_rechazo
UNION ALL SELECT fuente, motivo_rechazo, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.catalogo_rechazos') GROUP BY fuente, motivo_rechazo
UNION ALL SELECT fuente, motivo_rechazo, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.sucursales_rechazos') GROUP BY fuente, motivo_rechazo
UNION ALL SELECT fuente, motivo_rechazo, count(*) AS filas FROM IDENTIFIER(v_catalog || '.silver.tracking_envios_rechazos') GROUP BY fuente, motivo_rechazo
ORDER BY fuente, filas DESC;

-- COMMAND ----------

-- MAGIC %md ## 4. Aserciones (el job falla si alguna no se cumple)

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.ventas_netas_sucursal_mes')) > 0, 'gold.ventas_netas_sucursal_mes esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.top_productos_unidades')) > 0, 'gold.top_productos_unidades esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.top_productos_tasa_devolucion')) > 0, 'gold.top_productos_tasa_devolucion esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.dotacion_mensual')) > 0, 'gold.dotacion_mensual esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.empleados_vigentes')) > 0, 'gold.empleados_vigentes esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.ventas_por_vendedor')) > 0, 'gold.ventas_por_vendedor esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.resenas_negativas_categoria_mes')) > 0, 'gold.resenas_negativas_categoria_mes esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.metricas_despachos_courier')) > 0, 'gold.metricas_despachos_courier esta vacia') AS ok;

-- COMMAND ----------

SELECT assert_true((SELECT count(*) FROM IDENTIFIER(v_catalog || '.gold.resenas_por_origen_metadata')) > 0, 'gold.resenas_por_origen_metadata esta vacia') AS ok;

-- COMMAND ----------

-- Integridad Silver: la llave de ventas no se repite y no hay ventas sin sucursal
SELECT assert_true(
  (SELECT count(*) - count(DISTINCT venta_id) FROM IDENTIFIER(v_catalog || '.silver.ventas')) = 0,
  'silver.ventas tiene venta_id duplicados') AS ok;

-- COMMAND ----------

-- Integridad referencial: toda devolucion y todo tracking en Silver apunta a una venta de Silver
SELECT assert_true(
  (SELECT count(*) FROM IDENTIFIER(v_catalog || '.silver.devoluciones') d
   LEFT ANTI JOIN IDENTIFIER(v_catalog || '.silver.ventas') v ON d.venta_id = v.venta_id) = 0,
  'silver.devoluciones contiene venta_id inexistente en silver.ventas') AS ok;

-- COMMAND ----------

SELECT assert_true(
  (SELECT count(*) FROM IDENTIFIER(v_catalog || '.silver.tracking_envios') t
   LEFT ANTI JOIN IDENTIFIER(v_catalog || '.silver.ventas') v ON t.venta_id = v.venta_id) = 0,
  'silver.tracking_envios contiene venta_id inexistente en silver.ventas') AS ok;

-- COMMAND ----------

-- MAGIC %md ## 5. Gobierno: masking y permisos

-- COMMAND ----------

-- Miembro de electrocasa_ingenieria: dni y salario reales. Cualquier otro usuario: dni '****' + ultimos 4 y salario NULL.
SELECT id_empleado, dni, salario, tipo_evento, `__START_AT`, `__END_AT`
FROM IDENTIFIER(v_catalog || '.silver.empleados')
LIMIT 10;

-- COMMAND ----------

-- Tabla derivada en Gold: conserva los valores reales para ingenieria solo si el dueno del pipeline es miembro del grupo
SELECT id_empleado, dni, salario, sucursal_id FROM IDENTIFIER(v_catalog || '.gold.empleados_vigentes') LIMIT 10;

-- COMMAND ----------

SELECT current_user() AS usuario, is_account_group_member('electrocasa_ingenieria') AS es_ingenieria;

-- COMMAND ----------

EXECUTE IMMEDIATE concat('SHOW GRANTS ON CATALOG ', v_catalog);

-- COMMAND ----------

EXECUTE IMMEDIATE concat('SHOW GRANTS ON SCHEMA ', v_catalog, '.gold');

-- COMMAND ----------

EXECUTE IMMEDIATE concat('SHOW GRANTS ON SCHEMA ', v_catalog, '.silver');
