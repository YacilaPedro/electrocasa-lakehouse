"""Capa Silver: limpieza, tipado, deduplicacion, SCD2 y tablas de rechazos.

Convenciones (deben coincidir 1:1 con contratos/*.yml):
  * Los diccionarios REGLAS_* son las expectativas de Silver: {nombre: condicion SQL}.
  * ``<catalogo>.silver.<fuente>_rechazos`` se calcula con las MISMAS reglas
    (cleaning.solo_rechazados), de modo que contiene exactamente lo que Silver descarta.
  * Politica de las expectativas criticas: expect_all_or_drop. Las de dominio
    (metodo_pago, canal) son solo advertencia (expect_all).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils.cleaning import (
    a_decimal, a_entero, a_fecha, a_timestamp, con_ranking, corregir_salario,
    limpiar_texto, normalizar_canal, normalizar_categoria, normalizar_estado_envio,
    normalizar_metodo_pago, normalizar_tipo_evento, solo_rechazados,
)

CATALOG = spark.conf.get("catalog_name")


def tabla(esquema: str, nombre: str) -> str:
    return f"{CATALOG}.{esquema}.{nombre}"


# ---------------------------------------------------------------------------
# CATALOGO (bronze alimentado por COPY INTO) -> silver.catalogo
# ---------------------------------------------------------------------------
REGLAS_CATALOGO = {
    "producto_id_no_nulo": "producto_id IS NOT NULL",
    "precio_lista_positivo": "precio_lista IS NOT NULL AND precio_lista > 0",
}


@dp.view(name="catalogo_base")
def catalogo_base():
    b = spark.read.table(tabla("bronze", "catalogo"))
    activo = F.lower(F.trim(F.col("activo").cast("string")))
    df = b.select(
        limpiar_texto("producto_id").alias("producto_id"),
        limpiar_texto("nombre_producto").alias("nombre_producto"),
        normalizar_categoria("categoria").alias("categoria"),
        F.coalesce(limpiar_texto("marca"), F.lit("Sin marca")).alias("marca"),
        a_decimal("precio_lista", 12, 2).alias("precio_lista"),
        F.when(activo.isin("true", "1", "t", "si", "sí", "yes"), F.lit(True))
        .when(activo.isin("false", "0", "f", "no"), F.lit(False))
        .alias("activo"),
        F.col("_source_file"),
        F.col("_batch_id"),
        F.monotonically_increasing_id().alias("_row_seq"),
    )
    # 1) gana el snapshot mas reciente; 2) dentro del snapshot, el registro con precio valido;
    # 3) si persiste el empate, el ultimo en orden de lectura. En los duplicados de marzo solo
    # cambia precio_lista y el conteo de precios invalidos (42) es el mismo con cualquier orden.
    precio_valido = F.coalesce(F.col("precio_lista") > 0, F.lit(False))
    return con_ranking(
        df, ["producto_id"],
        [F.col("_batch_id").desc(), precio_valido.desc(), F.col("_row_seq").desc()],
    )


@dp.materialized_view(
    name=tabla("silver", "catalogo"),
    comment="Catalogo vigente: un registro por producto_id (snapshot mas reciente).",
)
@dp.expect_all_or_drop(REGLAS_CATALOGO)
def silver_catalogo():
    return spark.read.table("catalogo_base").filter("_rn = 1").drop("_rn", "_row_seq")


@dp.materialized_view(
    name=tabla("silver", "catalogo_rechazos"),
    comment="Productos vigentes descartados por precio_lista nulo o <= 0.",
)
def catalogo_rechazos():
    base = spark.read.table("catalogo_base").filter("_rn = 1")
    return solo_rechazados(base, REGLAS_CATALOGO, "catalogo").drop("_rn", "_row_seq")


# ---------------------------------------------------------------------------
# SUCURSALES (bronze alimentado por COPY INTO) -> silver.sucursales
# ---------------------------------------------------------------------------
REGLAS_SUCURSALES = {
    "sucursal_id_no_nulo": "sucursal_id IS NOT NULL",
}


@dp.view(name="sucursales_base")
def sucursales_base():
    b = spark.read.table(tabla("bronze", "sucursales"))
    df = b.select(
        limpiar_texto("sucursal_id").alias("sucursal_id"),
        limpiar_texto("nombre_sucursal").alias("nombre_sucursal"),
        limpiar_texto("distrito").alias("distrito"),
        F.initcap(limpiar_texto("ciudad")).alias("ciudad"),  # unico campo con casing inconsistente
        limpiar_texto("region").alias("region"),
        a_fecha("fecha_apertura").alias("fecha_apertura"),
        F.col("_source_file"),
        F.col("_batch_id"),
    )
    return con_ranking(df, ["sucursal_id"], [F.col("_batch_id").desc()])


@dp.materialized_view(name=tabla("silver", "sucursales"), comment="Sucursales con ciudad normalizada (initcap).")
@dp.expect_all_or_drop(REGLAS_SUCURSALES)
def silver_sucursales():
    return spark.read.table("sucursales_base").filter("_rn = 1").drop("_rn")


@dp.materialized_view(name=tabla("silver", "sucursales_rechazos"), comment="Sucursales sin sucursal_id.")
def sucursales_rechazos():
    return solo_rechazados(spark.read.table("sucursales_base").filter("_rn = 1"), REGLAS_SUCURSALES, "sucursales").drop("_rn")


# ---------------------------------------------------------------------------
# VENTAS -> silver.ventas
# ---------------------------------------------------------------------------
REGLAS_VENTAS = {
    "venta_id_no_nulo": "venta_id IS NOT NULL",
    "sucursal_id_no_nula": "sucursal_id IS NOT NULL",
    "cantidad_positiva": "cantidad > 0",
    "monto_total_positivo": "monto_total IS NOT NULL AND monto_total > 0",
    "fecha_venta_valida": "fecha_venta IS NOT NULL",
}
ADVERTENCIAS_VENTAS = {
    "metodo_pago_valido": "metodo_pago IN ('Tarjeta', 'Efectivo', 'Yape', 'Plin', 'Transferencia')",
    "canal_valido": "canal IN ('tienda_fisica', 'ecommerce')",
}


@dp.view(name="ventas_base")
def ventas_base():
    b = spark.read.table(tabla("bronze", "ventas"))
    en_catalogo = (
        spark.read.table(tabla("silver", "catalogo"))
        .select("producto_id").distinct()
        .withColumn("_en_catalogo", F.lit(True))
    )
    df = b.select(
        limpiar_texto("venta_id").alias("venta_id"),
        limpiar_texto("sucursal_id").alias("sucursal_id"),
        limpiar_texto("producto_id").alias("producto_id"),
        limpiar_texto("vendedor_id").alias("vendedor_id"),
        a_entero("cantidad").alias("cantidad"),
        a_decimal("monto_total", 12, 2).alias("monto_total"),
        normalizar_metodo_pago("metodo_pago").alias("metodo_pago"),
        a_timestamp("fecha_venta").alias("fecha_venta"),
        normalizar_canal("canal").alias("canal"),
        F.col("monto_total").alias("monto_total_raw"),
        F.col("fecha_venta").alias("fecha_venta_raw"),
        F.col("_source_file"),
        F.col("_batch_id"),
    )
    df = df.join(F.broadcast(en_catalogo), "producto_id", "left").withColumn(
        "producto_en_catalogo", F.coalesce(F.col("_en_catalogo"), F.lit(False))
    ).drop("_en_catalogo")
    return con_ranking(df, ["venta_id"], [F.col("_batch_id"), F.col("_source_file")])


@dp.materialized_view(
    name=tabla("silver", "ventas"),
    comment="Ventas limpias, tipadas y sin duplicados. Huerfanos de catalogo se conservan con producto_en_catalogo = false.",
)
@dp.expect_all_or_drop(REGLAS_VENTAS)
@dp.expect_all(ADVERTENCIAS_VENTAS)
def silver_ventas():
    return (
        spark.read.table("ventas_base")
        .filter("_rn = 1")
        .drop("_rn", "monto_total_raw", "fecha_venta_raw")
    )


@dp.materialized_view(
    name=tabla("silver", "ventas_rechazos"),
    comment="Ventas descartadas (duplicados por venta_id y reglas de calidad) con motivo_rechazo.",
)
def ventas_rechazos():
    reglas = {"venta_id_unico": "_rn = 1", **REGLAS_VENTAS}
    return solo_rechazados(spark.read.table("ventas_base"), reglas, "ventas").drop("_rn")


# ---------------------------------------------------------------------------
# EMPLEADOS -> silver.empleados (SCD Type 2 con AUTO CDC)
# ---------------------------------------------------------------------------
REGLAS_EMPLEADOS = {
    "id_empleado_no_nulo": "id_empleado IS NOT NULL",
    "tipo_evento_valido": "tipo_evento IN ('alta', 'transferencia', 'cambio_salario', 'baja')",
    "fecha_evento_no_nula": "fecha_evento IS NOT NULL",
}


def _empleados_normalizados(b):
    return b.select(
        limpiar_texto("id_empleado").alias("id_empleado"),
        limpiar_texto("nombre").alias("nombre"),
        limpiar_texto("dni").alias("dni"),
        F.lower(limpiar_texto("email")).alias("email"),
        corregir_salario("salario").alias("salario"),
        limpiar_texto("sucursal_id").alias("sucursal_id"),
        limpiar_texto("cargo").alias("cargo"),
        normalizar_tipo_evento("tipo_evento").alias("tipo_evento"),
        a_fecha("fecha_evento").alias("fecha_evento"),
        a_timestamp("fecha_registro").alias("fecha_registro"),
        F.col("_source_file"),
        F.col("_batch_id"),
    )


@dp.view(name="empleados_limpios")
@dp.expect_all_or_drop(REGLAS_EMPLEADOS)
def empleados_limpios():
    return _empleados_normalizados(spark.readStream.table(tabla("bronze", "empleados")))


@dp.table(
    name=tabla("silver", "empleados_rechazos"),
    comment="Eventos de empleados descartados (fecha_evento nula, id nulo o tipo_evento desconocido).",
)
def empleados_rechazos():
    normalizados = _empleados_normalizados(spark.readStream.table(tabla("bronze", "empleados")))
    return solo_rechazados(normalizados, REGLAS_EMPLEADOS, "empleados")


dp.create_streaming_table(
    name=tabla("silver", "empleados"),
    comment="Historial SCD2 de empleados. dni y salario protegidos con column masks.",
    schema=f"""
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING MASK {CATALOG}.silver.mask_dni,
        email STRING,
        salario DECIMAL(12,2) MASK {CATALOG}.silver.mask_salario,
        sucursal_id STRING,
        cargo STRING,
        tipo_evento STRING,
        fecha_evento DATE,
        fecha_registro TIMESTAMP,
        __START_AT STRUCT<fecha_evento: DATE, fecha_registro: TIMESTAMP>,
        __END_AT STRUCT<fecha_evento: DATE, fecha_registro: TIMESTAMP>
    """,
)

dp.create_auto_cdc_flow(
    target=tabla("silver", "empleados"),
    source="empleados_limpios",
    keys=["id_empleado"],
    sequence_by=F.struct("fecha_evento", "fecha_registro"),
    except_column_list=["_source_file", "_batch_id"],
    stored_as_scd_type=2,
)


# ---------------------------------------------------------------------------
# DEVOLUCIONES -> silver.devoluciones
# ---------------------------------------------------------------------------
REGLAS_DEVOLUCIONES = {
    "devolucion_id_no_nulo": "devolucion_id IS NOT NULL",
    "venta_id_no_nulo": "venta_id IS NOT NULL",
    "monto_reembolso_no_negativo": "monto_reembolso IS NOT NULL AND monto_reembolso >= 0",
    "venta_existe": "venta_id IS NULL OR monto_total_venta IS NOT NULL",
    "reembolso_no_excede_venta": "monto_total_venta IS NULL OR monto_reembolso <= monto_total_venta",
}


@dp.view(name="devoluciones_base")
def devoluciones_base():
    b = spark.read.table(tabla("bronze", "devoluciones"))
    ventas = spark.read.table(tabla("silver", "ventas")).select(
        "venta_id", F.col("monto_total").alias("monto_total_venta")
    )
    df = b.select(
        limpiar_texto("devolucion_id").alias("devolucion_id"),
        limpiar_texto("venta_id").alias("venta_id"),
        limpiar_texto("sucursal_id").alias("sucursal_id"),
        limpiar_texto("producto_id").alias("producto_id"),
        F.coalesce(limpiar_texto("motivo"), F.lit("no_especificado")).alias("motivo"),
        a_decimal("monto_reembolso", 12, 2).alias("monto_reembolso"),
        a_timestamp("fecha_devolucion").alias("fecha_devolucion"),
        F.col("_source_file"),
        F.col("_batch_id"),
    ).join(ventas, "venta_id", "left")
    return con_ranking(df, ["devolucion_id"], [F.col("_batch_id"), F.col("_source_file")])


@dp.materialized_view(
    name=tabla("silver", "devoluciones"),
    comment="Devoluciones validas contra silver.ventas. monto_total_venta es la verificacion referencial.",
)
@dp.expect_all_or_drop(REGLAS_DEVOLUCIONES)
def silver_devoluciones():
    return spark.read.table("devoluciones_base").filter("_rn = 1").drop("_rn")


@dp.materialized_view(
    name=tabla("silver", "devoluciones_rechazos"),
    comment="Devoluciones descartadas (duplicados y reglas de calidad) con motivo_rechazo.",
)
def devoluciones_rechazos():
    reglas = {"devolucion_id_unico": "_rn = 1", **REGLAS_DEVOLUCIONES}
    return solo_rechazados(spark.read.table("devoluciones_base"), reglas, "devoluciones").drop("_rn")


# ---------------------------------------------------------------------------
# RESENAS -> silver.resenas
# ---------------------------------------------------------------------------
REGLAS_RESENAS = {
    "resena_id_no_nulo": "resena_id IS NOT NULL",
    "calificacion_en_rango": "calificacion BETWEEN 1 AND 5",
}


@dp.view(name="resenas_base")
def resenas_base():
    b = spark.read.table(tabla("bronze", "resenas"))
    en_catalogo = (
        spark.read.table(tabla("silver", "catalogo"))
        .select("producto_id").distinct()
        .withColumn("_en_catalogo", F.lit(True))
    )
    respuestas = F.transform(
        F.col("respuestas"),
        lambda r: F.struct(
            F.coalesce(limpiar_texto(r["autor"]), F.lit("sin_autor")).alias("autor"),
            r["texto"].alias("texto"),
            r["fecha"].alias("fecha"),
        ),
    )
    df = b.select(
        limpiar_texto("resena_id").alias("resena_id"),
        limpiar_texto("producto_id").alias("producto_id"),
        limpiar_texto("cliente_id").alias("cliente_id"),
        a_entero("calificacion").alias("calificacion"),
        limpiar_texto("comentario").alias("comentario"),
        F.coalesce(F.col("tags"), F.array().cast("array<string>")).alias("tags"),
        respuestas.alias("respuestas"),
        a_timestamp("fecha_resena").alias("fecha_resena"),
        F.col("metadata"),
        F.col("_source_file"),
        F.col("_batch_id"),
    )
    df = df.join(F.broadcast(en_catalogo), "producto_id", "left").withColumn(
        "producto_en_catalogo", F.coalesce(F.col("_en_catalogo"), F.lit(False))
    ).drop("_en_catalogo")
    return con_ranking(df, ["resena_id"], [F.col("_batch_id"), F.col("_source_file")])


@dp.materialized_view(
    name=tabla("silver", "resenas"),
    comment="Reseñas validas (calificacion 1-5). Productos huerfanos se conservan con producto_en_catalogo = false.",
)
@dp.expect_all_or_drop(REGLAS_RESENAS)
def silver_resenas():
    return spark.read.table("resenas_base").filter("_rn = 1").drop("_rn")


@dp.materialized_view(
    name=tabla("silver", "resenas_rechazos"),
    comment="Reseñas descartadas (duplicados y calificacion fuera de rango) con motivo_rechazo.",
)
def resenas_rechazos():
    reglas = {"resena_id_unico": "_rn = 1", **REGLAS_RESENAS}
    return solo_rechazados(spark.read.table("resenas_base"), reglas, "resenas").drop("_rn")


# ---------------------------------------------------------------------------
# TRACKING_ENVIOS (bronze alimentado por Lakeflow Connect) -> silver.tracking_envios
# ---------------------------------------------------------------------------
REGLAS_TRACKING = {
    "evento_id_no_nulo": "evento_id IS NOT NULL",
    "tracking_id_no_nulo": "tracking_id IS NOT NULL",
    "venta_id_no_nulo": "venta_id IS NOT NULL",
    "estado_entrega_valido": "estado_entrega IN ('pendiente', 'en_transito', 'entregado', 'devuelto')",
    "venta_existe": "venta_id IS NULL OR sucursal_venta IS NOT NULL",
}


def _timestamp_flexible(df, nombre):
    """Si la columna ya es TIMESTAMP (SQL Server datetime2) se usa tal cual; si no, se parsea."""
    if dict(df.dtypes)[nombre].startswith("timestamp"):
        return F.col(nombre)
    return a_timestamp(nombre)


def _fecha_flexible(df, nombre):
    """fecha_actualizacion es DATE en origen (y puede ser nula); se tolera tambien TIMESTAMP o texto."""
    tipo = dict(df.dtypes)[nombre]
    if tipo == "date":
        return F.col(nombre)
    if tipo.startswith("timestamp"):
        return F.to_date(F.col(nombre))
    return a_fecha(nombre)


@dp.view(name="tracking_base")
def tracking_base():
    b = spark.read.table(tabla("bronze", "tracking_envios"))
    ventas = spark.read.table(tabla("silver", "ventas")).select(
        "venta_id", F.col("sucursal_id").alias("sucursal_venta")
    )
    df = b.select(
        limpiar_texto("evento_id").alias("evento_id"),
        limpiar_texto("tracking_id").alias("tracking_id"),
        limpiar_texto("venta_id").alias("venta_id"),
        F.initcap(limpiar_texto("courier")).alias("courier"),
        normalizar_estado_envio("estado_entrega").alias("estado_entrega"),
        limpiar_texto("estado_entrega").alias("estado_entrega_raw"),
        limpiar_texto("sucursal_origen").alias("sucursal_origen"),
        _fecha_flexible(b, "fecha_actualizacion").alias("fecha_actualizacion"),
        _timestamp_flexible(b, "fecha_modificacion").alias("fecha_modificacion"),
    ).join(ventas, "venta_id", "left")
    # Mensajes reprocesados (mismo cambio de estado con distinto evento_id): se conserva el primer
    # evento_id segun fecha_modificacion. Una fecha_actualizacion nula cuenta como un valor mas.
    return con_ranking(
        df,
        ["tracking_id", "estado_entrega", "fecha_actualizacion"],
        [F.col("fecha_modificacion"), F.col("evento_id")],
    )


@dp.materialized_view(
    name=tabla("silver", "tracking_envios"),
    comment="Eventos de despacho estandarizados y sin reprocesos. sucursal_venta es la verificacion referencial con silver.ventas.",
)
@dp.expect_all_or_drop(REGLAS_TRACKING)
def silver_tracking_envios():
    return spark.read.table("tracking_base").filter("_rn = 1").drop("_rn", "estado_entrega_raw")


@dp.materialized_view(
    name=tabla("silver", "tracking_envios_rechazos"),
    comment="Eventos de despacho huerfanos (venta inexistente) o con estado desconocido, con motivo_rechazo.",
)
def tracking_envios_rechazos():
    return solo_rechazados(spark.read.table("tracking_base").filter("_rn = 1"), REGLAS_TRACKING, "tracking_envios").drop("_rn")
