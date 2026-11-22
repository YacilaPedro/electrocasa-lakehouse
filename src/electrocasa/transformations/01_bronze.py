"""Capa Bronze: Auto Loader (cloudFiles) sobre el volume de landing.

Reglas que se respetan aqui:
  * Esquemas explicitos (StructType); nunca inferSchema.
  * Todo llega como STRING: la limpieza y el tipado son responsabilidad de Silver.
  * Sin logica de negocio: solo columnas tecnicas _ingested_at, _source_file, _batch_id
    (_batch_id = fecha del nombre del archivo; si no la trae, la de modificacion).
  * Sin checkpointLocation ni schemaLocation: Lakeflow los administra (Full Refresh seguro).
  * Metadatos de archivo via _metadata.file_name / _metadata.file_modification_time.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, VariantType

CATALOG = spark.conf.get("catalog_name")
LANDING = f"/Volumes/{CATALOG}/default/vol_landing"


def _strings(*nombres):
    return StructType([StructField(n, StringType(), True) for n in nombres])


SCHEMA_VENTAS = _strings(
    "venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad",
    "monto_total", "metodo_pago", "fecha_venta", "canal",
)
SCHEMA_EMPLEADOS = _strings(
    "id_empleado", "nombre", "dni", "email", "salario", "sucursal_id",
    "cargo", "tipo_evento", "fecha_evento", "fecha_registro",
)
SCHEMA_DEVOLUCIONES = _strings(
    "devolucion_id", "venta_id", "sucursal_id", "producto_id", "motivo",
    "monto_reembolso", "fecha_devolucion",
)
SCHEMA_RESENAS = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("calificacion", StringType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("respuestas", ArrayType(StructType([
        StructField("autor", StringType(), True),
        StructField("texto", StringType(), True),
        StructField("fecha", StringType(), True),
    ])), True),
    StructField("fecha_resena", StringType(), True),
    StructField("metadata", VariantType(), True),
])


def _autoloader(fuente: str, formato: str, schema: StructType, **opciones):
    lector = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .schema(schema)
    )
    for clave, valor in opciones.items():
        lector = lector.option(clave, valor)
    df = lector.load(f"{LANDING}/{fuente}/")
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn(
            "_batch_id",
            # Fecha del nombre (ventas_lote_01_2026-05-31.csv -> 2026-05-31): no depende del orden
            # ni del momento de subida. Si el nombre no trae fecha, se usa file_modification_time.
            F.coalesce(
                F.nullif(F.regexp_extract(F.col("_metadata.file_name"), r"([0-9]{4}-[0-9]{2}-[0-9]{2})", 1), F.lit("")),
                F.date_format(F.col("_metadata.file_modification_time"), "yyyy-MM-dd"),
            ),
        )
    )


@dp.table(
    name=f"{CATALOG}.bronze.ventas",
    comment="Ventas crudas (CSV, 3 lotes). Todo STRING; sin transformaciones.",
    table_properties={"quality": "bronze"},
)
def bronze_ventas():
    return _autoloader("ventas", "csv", SCHEMA_VENTAS, header="true", pathGlobFilter="*.csv")


@dp.table(
    name=f"{CATALOG}.bronze.empleados",
    comment="Eventos de empleados crudos (CSV, 3 lotes). Todo STRING; sin transformaciones.",
    table_properties={"quality": "bronze"},
)
def bronze_empleados():
    return _autoloader("empleados", "csv", SCHEMA_EMPLEADOS, header="true", pathGlobFilter="*.csv")


@dp.table(
    name=f"{CATALOG}.bronze.devoluciones",
    comment="Devoluciones crudas (CSV, 3 lotes). Todo STRING; sin transformaciones.",
    table_properties={"quality": "bronze"},
)
def bronze_devoluciones():
    return _autoloader("devoluciones", "csv", SCHEMA_DEVOLUCIONES, header="true", pathGlobFilter="*.csv")


@dp.table(
    name=f"{CATALOG}.bronze.resenas",
    comment="Reseñas crudas (JSON Lines, 2 lotes). metadata se conserva como VARIANT.",
    table_properties={"quality": "bronze"},
)
def bronze_resenas():
    return _autoloader("resenas", "json", SCHEMA_RESENAS, pathGlobFilter="*.json*")
