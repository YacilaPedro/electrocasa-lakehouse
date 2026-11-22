"""Capa Gold: respuestas a las preguntas de negocio (marzo a octubre 2026).

Todas son vistas materializadas que leen Silver. Las preguntas 1 a 6 mas el extra VARIANT
generan las tablas: ventas_netas_sucursal_mes, top_productos_unidades,
top_productos_tasa_devolucion, dotacion_mensual, ventas_por_vendedor,
resenas_negativas_categoria_mes, metricas_despachos_courier y resenas_por_origen_metadata,
mas empleados_vigentes (vista derivada de Empleados para demostrar el masking).
"""
from datetime import date

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")
INICIO = "2026-03-01"  # inclusive
FIN = "2026-11-01"     # exclusivo (hasta el 31 de octubre)


def silver(nombre: str):
    return spark.read.table(f"{CATALOG}.silver.{nombre}")


def gold(nombre: str) -> str:
    return f"{CATALOG}.gold.{nombre}"


def en_periodo(columna: str):
    c = F.col(columna)
    return (c >= F.to_timestamp(F.lit(INICIO))) & (c < F.to_timestamp(F.lit(FIN)))


# 1. Ventas netas por sucursal y mes ------------------------------------------------
@dp.materialized_view(
    name=gold("ventas_netas_sucursal_mes"),
    comment="Ventas brutas (ambos canales), devoluciones, ventas netas, nro. de ventas y ticket promedio.",
)
def ventas_netas_sucursal_mes():
    ventas = (
        silver("ventas").filter(en_periodo("fecha_venta"))
        .groupBy("sucursal_id", F.trunc("fecha_venta", "month").alias("mes"))
        .agg(
            F.sum("monto_total").alias("ventas_brutas"),
            F.countDistinct("venta_id").alias("cantidad_ventas"),
        )
    )
    devoluciones = (
        silver("devoluciones").filter(en_periodo("fecha_devolucion"))
        .groupBy("sucursal_id", F.trunc("fecha_devolucion", "month").alias("mes"))
        .agg(F.sum("monto_reembolso").alias("devoluciones"))
    )
    return (
        ventas.join(devoluciones, ["sucursal_id", "mes"], "full_outer")
        .withColumn("sucursal_id", F.coalesce("sucursal_id", F.lit("SIN_ASIGNAR")))
        .select(
            "sucursal_id", "mes",
            F.coalesce("ventas_brutas", F.lit(0)).cast("decimal(14,2)").alias("ventas_brutas"),
            F.coalesce("devoluciones", F.lit(0)).cast("decimal(14,2)").alias("devoluciones"),
            F.coalesce("cantidad_ventas", F.lit(0)).alias("cantidad_ventas"),
        )
        .withColumn("ventas_netas", (F.col("ventas_brutas") - F.col("devoluciones")).cast("decimal(14,2)"))
        .withColumn(
            "ticket_promedio",
            F.when(F.col("cantidad_ventas") > 0, F.col("ventas_brutas") / F.col("cantidad_ventas")).cast("decimal(14,2)"),
        )
    )


# 2. Top 10 productos ---------------------------------------------------------------
def _nombres_producto():
    return silver("catalogo").select("producto_id", "nombre_producto", "categoria")


def _ventas_por_producto():
    return (
        silver("ventas").filter(en_periodo("fecha_venta"))
        .groupBy("producto_id")
        .agg(
            F.sum("cantidad").alias("unidades_vendidas"),
            F.countDistinct("venta_id").alias("ventas"),
        )
    )


@dp.materialized_view(
    name=gold("top_productos_unidades"),
    comment="Top 10 productos por unidades vendidas (SUM(cantidad)).",
)
def top_productos_unidades():
    top = (
        _ventas_por_producto()
        .join(_nombres_producto(), "producto_id", "left")
        .orderBy(F.col("unidades_vendidas").desc(), F.col("producto_id"))
        .limit(10)
    )
    ranking = Window.orderBy(F.col("unidades_vendidas").desc(), F.col("producto_id"))
    return top.withColumn("ranking", F.row_number().over(ranking)).select(
        "ranking", "producto_id", "nombre_producto", "categoria", "unidades_vendidas", "ventas"
    )


@dp.materialized_view(
    name=gold("top_productos_tasa_devolucion"),
    comment="Top 10 productos por tasa de devolucion = devoluciones / ventas.",
)
def top_productos_tasa_devolucion():
    devoluciones = (
        silver("devoluciones").filter(en_periodo("fecha_devolucion") & F.col("producto_id").isNotNull())
        .groupBy("producto_id")
        .agg(F.countDistinct("devolucion_id").alias("devoluciones"))
    )
    top = (
        _ventas_por_producto()
        .join(devoluciones, "producto_id", "inner")
        .withColumn("tasa_devolucion", F.round(F.col("devoluciones") / F.col("ventas"), 4))
        .join(_nombres_producto(), "producto_id", "left")
        .orderBy(F.col("tasa_devolucion").desc(), F.col("ventas").desc(), F.col("producto_id"))
        .limit(10)
    )
    ranking = Window.orderBy(F.col("tasa_devolucion").desc(), F.col("ventas").desc(), F.col("producto_id"))
    return top.withColumn("ranking", F.row_number().over(ranking)).select(
        "ranking", "producto_id", "nombre_producto", "categoria", "ventas", "devoluciones", "tasa_devolucion"
    )


# 3. Dotacion mensual (SCD2) --------------------------------------------------------
@dp.materialized_view(
    name=gold("dotacion_mensual"),
    comment="Empleados activos por sucursal al ultimo dia de cada mes: alta en esa fecha o antes, sin baja hasta esa fecha.",
)
def dotacion_mensual():
    meses = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(F.lit(date(2026, 3, 1)), F.lit(date(2026, 10, 1)), F.expr("INTERVAL 1 MONTH"))
            ).alias("mes")
        )
        .withColumn("fin_mes", F.last_day("mes"))
        .alias("m")
    )
    historia = silver("empleados")
    # Un empleado solo cuenta si tiene un alta (los empleados cuyo alta se rechazo por fecha nula no cuentan).
    altas = historia.groupBy("id_empleado").agg(
        F.min(F.when(F.col("tipo_evento") == "alta", F.col("fecha_evento"))).alias("fecha_alta")
    )
    empleados = historia.join(altas, "id_empleado", "inner").alias("e")
    vigente = F.expr(
        "e.fecha_alta <= m.fin_mes "
        "AND e.`__START_AT`.fecha_evento <= m.fin_mes "
        "AND (e.`__END_AT` IS NULL OR e.`__END_AT`.fecha_evento > m.fin_mes) "
        "AND e.tipo_evento != 'baja'"
    )
    return (
        meses.join(empleados, vigente, "inner")
        .groupBy(
            F.col("m.mes").alias("mes"),
            F.col("m.fin_mes").alias("fin_mes"),
            F.coalesce(F.col("e.sucursal_id"), F.lit("SIN_ASIGNAR")).alias("sucursal_id"),
        )
        .agg(F.countDistinct("e.id_empleado").alias("empleados_activos"))
    )


# 3b. Vista derivada de Empleados (evidencia del masking en tablas derivadas) -------
@dp.materialized_view(
    name=gold("empleados_vigentes"),
    comment="Version vigente de cada empleado activo. Conserva dni y salario reales solo si el dueno del pipeline es de electrocasa_ingenieria.",
)
def empleados_vigentes():
    return (
        silver("empleados")
        .filter("`__END_AT` IS NULL AND tipo_evento != 'baja'")
        .select("id_empleado", "nombre", "dni", "email", "salario", "sucursal_id", "cargo", "fecha_evento")
    )


# 4. Ventas por vendedor (solo tienda fisica) ---------------------------------------
@dp.materialized_view(
    name=gold("ventas_por_vendedor"),
    comment="Monto y cantidad por vendedor, sucursal vigente en fecha_venta (SCD2) y mes. Solo tienda_fisica.",
)
def ventas_por_vendedor():
    v = (
        silver("ventas")
        .filter((F.col("canal") == "tienda_fisica") & en_periodo("fecha_venta"))
        .alias("v")
    )
    e = silver("empleados").filter("tipo_evento != 'baja'").alias("e")
    vigente = F.expr(
        "v.vendedor_id = e.id_empleado "
        "AND CAST(v.fecha_venta AS DATE) >= e.`__START_AT`.fecha_evento "
        "AND (e.`__END_AT` IS NULL OR CAST(v.fecha_venta AS DATE) < e.`__END_AT`.fecha_evento)"
    )
    return (
        v.join(e, vigente, "left")
        .groupBy(
            F.col("v.vendedor_id").alias("vendedor_id"),
            F.coalesce(F.col("e.sucursal_id"), F.lit("SIN_ASIGNAR")).alias("sucursal_id"),
            F.trunc("v.fecha_venta", "month").alias("mes"),
        )
        .agg(
            F.sum("v.monto_total").cast("decimal(14,2)").alias("monto_total"),
            F.sum("v.cantidad").alias("cantidad"),
        )
    )


# 5. % de reseñas negativas por categoria y mes -------------------------------------
@dp.materialized_view(
    name=gold("resenas_negativas_categoria_mes"),
    comment="% de reseñas con calificacion 1 o 2 por categoria (ultimo snapshot) y mes. Excluye productos fuera de catalogo.",
)
def resenas_negativas_categoria_mes():
    r = silver("resenas").filter(en_periodo("fecha_resena"))
    return (
        r.join(silver("catalogo").select("producto_id", "categoria"), "producto_id", "inner")
        .groupBy("categoria", F.trunc("fecha_resena", "month").alias("mes"))
        .agg(
            F.count("*").alias("total_resenas"),
            F.sum(F.when(F.col("calificacion") <= 2, 1).otherwise(0)).alias("resenas_negativas"),
        )
        .withColumn(
            "pct_negativas",
            F.round(F.col("resenas_negativas") * 100.0 / F.col("total_resenas"), 2),
        )
    )


# 6. Metricas de despacho por courier -----------------------------------------------
@dp.materialized_view(
    name=gold("metricas_despachos_courier"),
    comment="Horas promedio pendiente -> entregado (solo envios que llegaron a entregado) y % de envios devueltos sobre finalizados.",
)
def metricas_despachos_courier():
    # tracking_envios en Silver ya esta sin mensajes reprocesados ni estados desconocidos.
    por_envio = silver("tracking_envios").groupBy("tracking_id").agg(
        F.min("courier").alias("courier"),
        F.min(F.when(F.col("estado_entrega") == "pendiente", F.col("fecha_modificacion"))).alias("t_pendiente"),
        F.min(F.when(F.col("estado_entrega") == "entregado", F.col("fecha_modificacion"))).alias("t_entregado"),
        F.max(F.when(F.col("estado_entrega") == "entregado", 1).otherwise(0)).alias("hubo_entregado"),
        F.max(F.when(F.col("estado_entrega") == "devuelto", 1).otherwise(0)).alias("hubo_devuelto"),
    )
    horas = (F.unix_timestamp("t_entregado") - F.unix_timestamp("t_pendiente")) / 3600.0
    return (
        por_envio.groupBy("courier")
        .agg(
            F.round(F.avg(F.when(F.col("hubo_entregado") == 1, horas)), 2).alias("horas_promedio_entrega"),
            F.sum("hubo_entregado").alias("envios_entregados"),
            F.sum("hubo_devuelto").alias("envios_devueltos"),
            F.sum(F.when((F.col("hubo_entregado") == 1) | (F.col("hubo_devuelto") == 1), 1).otherwise(0)).alias("envios_finalizados"),
        )
        .withColumn(
            "pct_devueltos",
            F.when(
                F.col("envios_finalizados") > 0,
                F.round(F.col("envios_devueltos") * 100.0 / F.col("envios_finalizados"), 2),
            ),
        )
    )


# 7. Extra VARIANT: reseñas por origen de metadata ----------------------------------
@dp.materialized_view(
    name=gold("resenas_por_origen_metadata"),
    comment="Reseñas por metadata:origen (app/web/tienda), version y SO de la app y navegador web (columna VARIANT).",
)
def resenas_por_origen_metadata():
    r = silver("resenas").filter(en_periodo("fecha_resena"))
    return (
        r.select(
            F.expr("try_variant_get(metadata, '$.origen', 'string')").alias("origen"),
            F.expr("try_variant_get(metadata, '$.app.version', 'string')").alias("version_app"),
            F.expr("try_variant_get(metadata, '$.app.so', 'string')").alias("so_app"),
            F.expr("try_variant_get(metadata, '$.navegador', 'string')").alias("navegador"),
            "calificacion",
        )
        .groupBy("origen", "version_app", "so_app", "navegador")
        .agg(
            F.count("*").alias("total_resenas"),
            F.round(F.avg("calificacion"), 2).alias("calificacion_promedio"),
            F.round(
                F.sum(F.when(F.col("calificacion") <= 2, 1).otherwise(0)) * 100.0 / F.count("*"), 2
            ).alias("pct_negativas"),
        )
    )
