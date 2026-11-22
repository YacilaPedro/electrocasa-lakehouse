# ElectroCasa Lakehouse · Proyecto Integrador (Edición 15)

Plataforma de datos de punta a punta para ElectroCasa, una cadena de electrodomésticos con 40 sucursales en Perú. Integra seis fuentes más la tabla de sucursales en un lakehouse medallion sobre Databricks (Unity Catalog, Lakeflow Spark Declarative Pipelines, Lakeflow Connect, Lakeflow Jobs y Declarative Automation Bundles) y responde las seis preguntas de negocio de marzo a octubre de 2026.

> **Estado:** el código está escrito y validado en sintaxis. Las secciones marcadas con `[EVIDENCIA]` se completan con capturas o logs de tu ejecución en Databricks.

## 1. Caso y arquitectura

Hoy cada sucursal exporta sus ventas por separado, RR.HH. lleva su propio sistema, atención al cliente lee las reseñas a mano y el seguimiento de despachos vive en la base del courier. Este proyecto construye la fuente única de verdad.

```
Landing Volume (default.vol_landing/<fuente>/)
   ├─ Auto Loader ──► Streaming Tables ─┐   ventas · empleados · devoluciones · reseñas
   └─ COPY INTO (tarea del job) ────────┤   catálogo · sucursales   → tablas Delta en bronze
Azure SQL dbo.TrackingEnvios            ▼
   └─ Lakeflow Connect query-based ──► BRONZE ──► SILVER ──► GOLD ──► analistas / auditoría
                                         (crudo)   (limpio, SCD2,   (6 preguntas
                                                    masking,         + VARIANT)
                                                    *_rechazos)
```

| Capa | Qué contiene |
|---|---|
| Bronze | Datos crudos, todo `STRING`, más `_ingested_at`, `_source_file` y `_batch_id` (fecha del nombre del archivo). Sin lógica de negocio. |
| Silver | Tipado, normalización, deduplicación, SCD2 de empleados, column masks y una tabla `<fuente>_rechazos` por fuente (con `fuente`, `motivo_rechazo`, `fecha_procesamiento`, `_source_file`, `_batch_id`). |
| Gold | `ventas_netas_sucursal_mes`, `top_productos_unidades`, `top_productos_tasa_devolucion`, `dotacion_mensual`, `ventas_por_vendedor`, `resenas_negativas_categoria_mes`, `metricas_despachos_courier`, `resenas_por_origen_metadata` y `empleados_vigentes` (tabla derivada para demostrar el masking). |

Estructura del repositorio:

```
databricks.yml                      Bundle y targets dev / prod
resources/electrocasa_pipeline.yml  Pipeline medallion + pipeline de ingesta de Tracking (Lakeflow Connect)
resources/electrocasa_job.yml       Job secuencial con trigger por llegada de archivo
src/electrocasa/transformations/    01_bronze.py · 02_silver.py · 03_gold.py
src/electrocasa/utils/cleaning.py   Funciones de limpieza reutilizables
contratos/                          Un contrato de datos YAML por fuente (coinciden con el código)
notebooks/                          00_setup.ipynb · 01_copy_into_bronze.sql · 02_validacion_y_gobierno.sql
```

## 2. Ingesta: método por fuente

| Fuente | Método | Por qué |
|---|---|---|
| Ventas, Devoluciones | Auto Loader (CSV, schema explícito) | Llegan por lotes de miles de filas durante todo el año; los archivos son nuevos y no se reescriben. Auto Loader detecta archivos nuevos solo y su checkpoint lo administra el pipeline. |
| Empleados | Auto Loader (CSV, schema explícito) | Eventos que llegan por lote, incluidos los registrados tarde, y alimentan un `AUTO CDC` que necesita una fuente de streaming. |
| Reseñas | Auto Loader (JSON Lines) | Archivos nuevos por lote; `metadata` se ingiere como `VARIANT`. |
| Catálogo | `COPY INTO` (JSON `multiLine`, `primitivesAsString`) | Llega como **snapshot completo** ocasional (marzo y agosto), no como flujo continuo. `COPY INTO` es idempotente por archivo y el snapshot se resuelve en Silver. |
| Sucursales | `COPY INTO` (CSV con encabezado) | Un archivo único de 40 filas que casi no cambia. |
| Tracking de envíos | Lakeflow Connect, conector query-based de SQL Server | Obligatorio. Cursor `fecha_modificacion`, llave primaria `evento_id`, **append only**. |

`COPY INTO` corre en una tarea del job, fuera del pipeline, y escribe en tablas Delta de `bronze`; el pipeline las lee. Ninguna ruta de checkpoint ni `schemaLocation` se configura a mano.

**Tracking: append only en vez de SCD1.** Cada fila es un evento inmutable con `evento_id` único, y Gold necesita la historia (primer `pendiente` y primer `entregado` de cada envío). SCD1 sobrescribiría filas por llave sin aportar nada, porque la llave no se repite. La llave primaria es `evento_id`, nunca `tracking_id` (se repite por diseño y perdería eventos).

## 3. Decisiones que tomé y por qué

| Tema | Decisión | Justificación |
|---|---|---|
| Producto huérfano en **ventas** | Se conserva con `producto_en_catalogo = false` | Descartarlo subestimaría las ventas por sucursal (70 ventas). |
| Producto huérfano en **reseñas** | Se conserva con `producto_en_catalogo = false`; Gold lo excluye con `INNER JOIN` | Q5 exige excluirlo del numerador y del denominador, pero la reseña sigue siendo un dato válido. |
| Devolución o tracking con **venta inexistente o rechazada** | Se rechazan (`venta_existe`) | Si la venta no entra en ventas brutas, su reembolso tampoco debe restar en netas. Afecta a unas 148 devoluciones (30 inexistentes + 118 sobre ventas rechazadas). |
| Mismo **DNI en dos empleados** | Se conservan ambos; la llave es `id_empleado` | Es un error de digitación, no prueba de que sean la misma persona. Los 6 DNI repetidos se documentan en el contrato. |
| **Salarios** fuera de rango | Si `salario > 50000` se divide entre 100 | El rango normal es 1,201.85–8,011.21 y los 18 casos son un error de escala evidente (136,702 a 310,523). |
| **Máscara** | Solo `electrocasa_ingenieria` ve `dni` y `salario` reales. Los demás ven `****` + últimos 4 del DNI y `NULL` en salario | Permite identificar un registro sin exponer el dato. |
| **Descontinuados** (`activo: false`) | Se conservan marcados | Siguen apareciendo en ventas y reseñas históricas. |
| **Duplicados del catálogo de marzo** | Gana el snapshot más reciente; dentro del snapshot, el precio válido y luego el último registro leído | En los 25 duplicados solo cambia `precio_lista` y no hay fecha para desempatar; el conteo de precios inválidos (42) no depende del orden. |
| Fecha con barras | `DD/MM/YYYY` | Así lo define el perfilamiento (`09/05/2026`). |
| `_batch_id` | Fecha del nombre del archivo (`2026-05-31`) | No depende del orden ni del momento de subida. |

### Expectativas de calidad y política de violación

Elegí **descarte** (`expect_all_or_drop`) para toda regla que afecta un cálculo de Gold, con la fila siempre consultable en `<fuente>_rechazos`. No usé **falla** en ninguna: un puñado de filas malas no debe frenar un pipeline que procesa miles. La falla queda en la tarea de validación, que sí detiene el job si Gold queda vacío. La **advertencia** (`expect_all`) se reserva a dominios que no condicionan ningún monto.

| Fuente | Regla | Política | Por qué |
|---|---|---|---|
| Ventas | `venta_id_no_nulo`, `venta_id_unico` | descarte | Sin llave única se duplicarían ventas. |
| Ventas | `sucursal_id_no_nula` | descarte | Sin sucursal no entra en Q1. |
| Ventas | `cantidad_positiva` | descarte | Q2 suma `cantidad`; 0 no es una venta. |
| Ventas | `monto_total_positivo` | descarte | Es la expectativa clave de Q1 (nulos, ceros y negativos). |
| Ventas | `fecha_venta_valida` | descarte | Sin fecha no hay mes. |
| Ventas | `metodo_pago_valido`, `canal_valido` | advertencia | No cambian ningún monto; se observan sin perder ventas. |
| Catálogo | `precio_lista_positivo` | descarte | Precio numérico y mayor a 0 (42 en marzo, 51 en agosto). |
| Empleados | `id_empleado_no_nulo`, `fecha_evento_no_nula`, `tipo_evento_valido` | descarte | Sin ellas no se puede ubicar el evento en la historia. |
| Reseñas | `calificacion_en_rango` | descarte | Q5 calcula un porcentaje sobre 1 a 5. |
| Devoluciones | `monto_reembolso_no_negativo`, `reembolso_no_excede_venta`, `venta_existe` | descarte | Un reembolso inválido distorsiona las ventas netas. La vista `devoluciones_base` hace el join con `silver.ventas` y la expectativa se declara sobre el resultado. |
| Tracking | `estado_entrega_valido`, `venta_existe` | descarte | Un estado desconocido o un envío sin venta falsea Q6. |
| Sucursales | `sucursal_id_no_nulo` | descarte | Llave de la dimensión. |

### Historización de Empleados (SCD Type 2)

Usé `AUTO CDC` (`stored_as_scd_type=2`) con `sequence_by = struct(fecha_evento, fecha_registro)`, nunca `current_timestamp()`. Esto resuelve lo que traen los datos: 38 pares con dos eventos el mismo día (siempre un alta y un cambio de salario; `fecha_registro` los desempata) y 63 eventos de junio a agosto que RR.HH. registró en el último lote (el CDC los reubica y cierra la vigencia de la versión anterior). Para dotación, Gold exige además un alta: 12 empleados cuya alta cayó en una fila sin `fecha_evento` quedan fuera.

## 4. Orquestación

Una sola job (`electrocasa_job`) con cuatro tareas **en secuencia**: `copy_into_bronze` (notebook SQL) → `ingest_tracking` (pipeline de Lakeflow Connect) → `medallion_pipeline` (pipeline) → `validacion_calidad` (notebook SQL). En Free Edition solo puede haber un pipeline activo por tipo, así que nunca corren en paralelo.

* **Trigger: llegada de archivo** al volume de landing (espera 120 s tras el último cambio; mínimo 300 s entre disparos). Los datos llegan por lotes de archivos, así que el disparo por archivo evita correr en vacío y evita una frecuencia programada alta que gaste cuota. Crea y activa el trigger unos minutos antes de subir el lote.
* **Reintentos:** 2 en las ingestas, 1 en el pipeline principal. **Alertas:** correo ante falla y ante duración mayor a 90 minutos, al usuario que despliega (`alert_email`, configurable con `--var alert_email=<correo>`).
* **Validación:** muestra los conteos de Bronze por lote, de Silver, Gold y rechazos por fuente y motivo; guarda cada corrida en `default.log_conteos`; y **falla con `assert_true`** si alguna tabla Gold queda vacía, si `silver.ventas` repite `venta_id` o si devoluciones o tracking apuntan a ventas que no están en Silver.

### Compute

Todo es **serverless**: los pipelines, el conector y los notebooks (incluidos los SQL, que corren como notebooks sobre compute serverless). Los volúmenes son de decenas de miles de filas y las corridas son por lote, así que un cluster dedicado se pagaría ocioso la mayor parte del tiempo. Ningún recurso usa clusters all-purpose y ninguno corre en modo continuo.

## 5. Gobierno y seguridad

Todas las tablas son managed. Grupos creados desde **Settings > Identity and access > Groups**.

| Grupo | Acceso |
|---|---|
| `electrocasa_ingenieria` | Lectura y escritura sobre bronze, silver y gold; lectura/escritura del volume |
| `electrocasa_analistas` | Solo lectura sobre gold |
| `electrocasa_auditoria` | Solo lectura sobre gold y acceso a la evidencia de auditoría (`system.access`) |

La máscara de `dni` y `salario` se declara en el schema de `silver.empleados` (cláusula `MASK`, funciones `mask_dni` y `mask_salario` con `is_account_group_member('electrocasa_ingenieria')`). El **dueño del pipeline debe ser miembro de `electrocasa_ingenieria`**: si no, las tablas derivadas (como `gold.empleados_vigentes`) guardan los valores ya enmascarados.

## 6. Cómo desplegarlo

1. `databricks auth login --host <url-del-workspace>`
2. Crear los tres grupos (Settings > Identity and access > Groups) y agregarte a `electrocasa_ingenieria`.
3. Ejecutar `notebooks/00_setup.ipynb`: crea catálogos, schemas, volume y subcarpetas, las funciones de máscara y los permisos, y demuestra `REVOKE`.
4. Crear la Connection de Unity Catalog de tipo SQL Server (Catalog > Connections) con los datos del docente y el nombre `conn_azure_sql_electrocasa` (variable `connection_name`). Las credenciales viven solo en la Connection.
5. `databricks bundle validate -t dev`, `databricks bundle deploy -t dev`.
6. Subir **solo el lote 1** de cada fuente (más el snapshot de marzo del catálogo y `sucursales.csv`) y ejecutar `databricks bundle run electrocasa_job -t dev`. Repetir con los lotes 2 y 3.
7. Producción: `-t prod` (catálogo `electrocasa_prod`). No correr los pipelines de dos targets a la vez en Free Edition.

Si no logras crear los grupos (Free Edition): en `00_setup.ipynb` pon `USAR_GRUPO_EN_MASCARA = False` y tu correo en `CORREO_ALTERNATIVO`; la máscara compara con `current_user()` y se documenta aquí.

## 7. Conteos después de cada lote

**Bronze (exacto, por archivo)**

| Fuente | Lote 1 | Lote 2 | Lote 3 |
|---|---:|---:|---:|
| ventas | 6,783 | 6,839 | 4,618 |
| empleados | 2,257 | 1,267 | 924 |
| devoluciones | 1,387 | 1,704 | 1,126 |
| reseñas | 5,292 | 3,988 | — |
| catálogo | 2,305 (marzo) | 2,400 (agosto) | — |
| sucursales | 40 | — | — |
| tracking | 15,139 (una carga) | — | — |

**Silver acumulado (valores esperados, calculados con Python sobre los archivos de datos con las mismas reglas del pipeline).** Compáralos con la columna *Real* que salga de `default.log_conteos`.

| Tabla | Después del lote 1 (esperado / real) | Después del lote 2 | Después del lote 3 |
|---|---|---|---|
| `silver.ventas` / `ventas_rechazos` | 6,604 / 179 · `[EVIDENCIA]` | 13,118 / 504 · `[EVIDENCIA]` | 17,488 / 752 · `[EVIDENCIA]` |
| `silver.devoluciones` / `devoluciones_rechazos` | 1,301 / 86 · `[EVIDENCIA]` | 2,877 / 214 · `[EVIDENCIA]` | 3,908 / 309 · `[EVIDENCIA]` |
| `silver.empleados` (versiones) / `empleados_rechazos` | 2,239 / 18 · `[EVIDENCIA]` | 3,495 / 29 · `[EVIDENCIA]` | 4,410 / 38 · `[EVIDENCIA]` |
| `silver.resenas` / `resenas_rechazos` | 5,226 / 66 · `[EVIDENCIA]` | 9,097 / 183 · `[EVIDENCIA]` | (sin lote 3) |
| `silver.catalogo` / `catalogo_rechazos` | 2,238 / 42 · `[EVIDENCIA]` | 2,349 / 51 · `[EVIDENCIA]` | — |
| `silver.sucursales` | 40 · `[EVIDENCIA]` | — | — |

Las reingestas no duplican filas: tras el lote 3, `silver.ventas` tiene 17,488 `venta_id` distintos de los 18,000 únicos del origen (los 512 restantes están en rechazos). Los eventos de llegada tardía de Empleados quedan en su lugar cronológico del SCD2.

## 8. Evidencia de ejecución

* **Tracking, dos corridas del conector:** `[EVIDENCIA]` `DESCRIBE HISTORY <catalogo>.bronze.tracking_envios`: la primera corrida carga los 15,139 eventos y la segunda no agrega filas nuevas.
* **Pipeline y job ejecutados:** `[EVIDENCIA]` captura del grafo del pipeline y de la corrida del job con sus cuatro tareas en verde.
* **Despliegue del bundle:** `[EVIDENCIA]` salida de `databricks bundle validate` y `deploy` (o panel Deployments / extensión de VS Code).
* **Monitoreo:** `[EVIDENCIA]` consulta al event log del pipeline, por ejemplo `SELECT timestamp, event_type, message FROM event_log(TABLE(<catalogo>.silver.ventas))`, o al historial de corridas del job. Si `system.lakeflow` o `system.billing` no están disponibles, pega aquí el error y usa el event log o el historial del job en la interfaz.
* **REVOKE:** `[EVIDENCIA]` `SHOW GRANTS ON SCHEMA <catalogo>.silver` antes y después de revocar `SELECT` a `electrocasa_analistas` (lo imprime `00_setup.ipynb`).
* **Auditoría:** `[EVIDENCIA]` error de los `GRANT` sobre `system.access` (esperado en Free Edition) y captura de la pestaña Lineage en Catalog Explorer.
* **Masking:** `[EVIDENCIA]` `SELECT dni, salario FROM <catalogo>.gold.empleados_vigentes` ejecutada por un miembro de `electrocasa_ingenieria` (valores reales) y por otro usuario (`****NNNN` y `NULL`).

## 9. Retos opcionales implementados

* **`VARIANT` en Reseñas:** `metadata` se ingiere como `VARIANT` y `gold.resenas_por_origen_metadata` lo consulta con `try_variant_get` (`origen`, `app.version`, `app.so`, `navegador`).
* **Conector en el bundle:** el pipeline de ingesta de Tracking está declarado en `resources/electrocasa_pipeline.yml`, no creado desde la interfaz.

## 10. Supuestos y limitaciones

* **No se ejecutó en Databricks desde donde se escribió el código.** Validé la sintaxis de Python, YAML y notebooks y calculé offline los conteos esperados; la ejecución real puede revelar ajustes (por ejemplo, la configuración del conector query-based, que es reciente: `databricks bundle validate` confirma los campos según tu versión de CLI).
* **Estados de tracking:** el mapa reconoce `pendiente`, `en_transito` (incluye `en camino`), `entregado` y `devuelto`. Si el `SELECT DISTINCT estado_entrega` de Bronze muestra otra variante, se agrega en `cleaning.normalizar_estado_envio`; mientras tanto va a `tracking_envios_rechazos`.
* **Baja con fecha nula:** una baja rechazada por no tener `fecha_evento` deja al empleado activo en Gold (3 casos en los datos).
* **Tasa de devolución:** por producto, devoluciones / ventas, sin umbral mínimo de ventas; un producto con pocas ventas puede quedar arriba.
* **Tracking, mensajes reprocesados:** la clave `(tracking_id, estado_entrega, fecha_actualizacion)` trata una `fecha_actualizacion` nula como un valor más.
* **Row-level security y costos medidos (`system.billing`):** no implementados.
