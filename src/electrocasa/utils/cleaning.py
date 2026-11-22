"""Funciones de limpieza reutilizables para la capa Silver de ElectroCasa.

Todas operan solo con expresiones de columna (sin UDFs) y evitan ``CAST`` directos
sobre texto sucio: con ANSI activo (serverless) un ``CAST`` invalido aborta el pipeline,
asi que primero se valida con regex y recien despues se castea.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

FORMATOS_FECHA = (
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "dd/MM/yyyy",
)


def _col(c) -> Column:
    return F.col(c) if isinstance(c, str) else c


def limpiar_texto(c) -> Column:
    """trim + cadena vacia -> NULL. Siempre devuelve STRING."""
    return F.nullif(F.trim(_col(c).cast("string")), F.lit(""))


def sin_acentos(c) -> Column:
    """minusculas, sin espacios laterales y sin tildes (para comparar variantes)."""
    return F.translate(F.lower(F.trim(_col(c).cast("string"))), "áéíóúüñ", "aeiouun")


def a_decimal(c, precision: int = 12, escala: int = 2) -> Column:
    """Normaliza numeros con formatos mixtos a DECIMAL(precision, escala).

    Soporta ``"3.468,69"`` (punto de miles + coma decimal), ``"407,98"``,
    ``"-1602.5"`` y ``"S/ 1,474.75"`` -> la regla es: si hay coma Y punto, el ultimo
    separador es el decimal; si solo hay coma, la coma es el decimal.
    Todo lo que no sea numerico queda en NULL.
    """
    x = F.regexp_replace(F.trim(_col(c).cast("string")), r"[^0-9,.\-]", "")
    tiene_coma = x.contains(",")
    tiene_punto = x.contains(".")
    coma_es_decimal = tiene_coma & (~tiene_punto | (F.locate(",", x) > F.locate(".", x)))
    normalizado = (
        F.when(coma_es_decimal, F.regexp_replace(F.regexp_replace(x, r"\.", ""), ",", "."))
        .when(tiene_coma, F.regexp_replace(x, ",", ""))
        .otherwise(x)
    )
    return F.when(
        normalizado.rlike(r"^-?[0-9]+(\.[0-9]+)?$"),
        normalizado.cast(DecimalType(precision, escala)),
    )


def a_entero(c) -> Column:
    """Texto -> INT (acepta ``"3"`` y ``"3.0"``); lo demas queda NULL."""
    x = F.trim(_col(c).cast("string"))
    entero = F.regexp_extract(x, r"^(-?[0-9]{1,9})(\.0+)?$", 1)
    return F.when(entero != "", entero.cast("int"))


def a_timestamp(c) -> Column:
    """Texto con formatos mixtos -> TIMESTAMP.

    ``YYYY-MM-DDTHH:mm:ss``, ``YYYY-MM-DD HH:mm:ss``, ``YYYY-MM-DD`` y ``DD/MM/YYYY``.
    El formato con barras se interpreta siempre como dia/mes/anio.
    """
    x = F.trim(_col(c).cast("string"))
    return F.coalesce(*[F.try_to_timestamp(x, F.lit(f)) for f in FORMATOS_FECHA])


def a_fecha(c) -> Column:
    return F.to_date(a_timestamp(c))


def normalizar_metodo_pago(c) -> Column:
    x = sin_acentos(c)
    return (
        F.when(x.isin("tarjeta", "tc", "td", "tarjeta de credito", "tarjeta de debito"), F.lit("Tarjeta"))
        .when(x.isin("efectivo", "efv"), F.lit("Efectivo"))
        .when(x == "yape", F.lit("Yape"))
        .when(x == "plin", F.lit("Plin"))
        .when(x.isin("transferencia", "trf"), F.lit("Transferencia"))
    )


def normalizar_canal(c) -> Column:
    x = F.regexp_replace(sin_acentos(c), r"[\s\-]+", "_")
    return F.when(x.isin("tienda_fisica", "ecommerce"), x)


def normalizar_tipo_evento(c) -> Column:
    x = F.regexp_replace(sin_acentos(c), r"[\s\-]+", "_")
    return (
        F.when(x == "alta", F.lit("alta"))
        .when(x == "transferencia", F.lit("transferencia"))
        .when(x == "cambio_salario", F.lit("cambio_salario"))
        .when(x.isin("baja", "cese"), F.lit("baja"))
    )


def normalizar_categoria(c) -> Column:
    x = sin_acentos(c)
    return (
        F.when(x.contains("cocina"), F.lit("Cocina"))
        .when(x.contains("blanca"), F.lit("Línea blanca"))
        .when(x.contains("climat"), F.lit("Climatización"))
        .when(x.contains("entreten"), F.lit("Entretenimiento"))
        .when(x.contains("electr"), F.lit("Electrónica"))
    )


def normalizar_estado_envio(c) -> Column:
    """Estados de tracking -> pendiente, en_transito, entregado, devuelto.

    'En camino' / 'EN_CAMINO' son sinonimos de en_transito. Lo no reconocido queda NULL
    y la regla estado_entrega_valido lo envia a silver.tracking_envios_rechazos.
    """
    x = F.regexp_replace(sin_acentos(c), r"[\s\-]+", "_")
    return (
        F.when(x == "pendiente", F.lit("pendiente"))
        .when(x.isin("en_transito", "en_camino", "entransito", "encamino"), F.lit("en_transito"))
        .when(x == "entregado", F.lit("entregado"))
        .when(x == "devuelto", F.lit("devuelto"))
    )


def corregir_salario(c) -> Column:
    """Salarios multiplicados por 100 (> 50,000) se dividen entre 100."""
    s = a_decimal(c, 12, 2)
    return F.when(s > 50000, (s / F.lit(100)).cast(DecimalType(12, 2))).otherwise(s)


def con_ranking(df: DataFrame, llaves, orden) -> DataFrame:
    """Agrega ``_rn`` (1 = registro a conservar) segun ``orden`` (lista de Column)."""
    ventana = Window.partitionBy(*llaves).orderBy(*orden)
    return df.withColumn("_rn", F.row_number().over(ventana))


def solo_rechazados(df: DataFrame, reglas: dict, fuente: str) -> DataFrame:
    """Devuelve las filas que incumplen alguna regla, con ``motivo_rechazo``.

    ``reglas`` es ``{nombre_expectativa: condicion_sql}``: los mismos nombres y
    condiciones que usan las expectativas de Silver, de modo que ``<fuente>_rechazos``
    contiene exactamente lo que Silver descarta. Cada fila lleva ``fuente``, ``motivo_rechazo``
    (todas las reglas que fallo) y ``fecha_procesamiento``; ``_source_file`` y ``_batch_id``
    identifican el archivo y el lote de origen (trazabilidad para reprocesar).
    """
    fallos = [
        F.when(~F.coalesce(F.expr(condicion), F.lit(False)), F.lit(nombre))
        for nombre, condicion in reglas.items()
    ]
    return (
        df.withColumn("motivo_rechazo", F.concat_ws(", ", *fallos))
        .filter(F.col("motivo_rechazo") != "")
        .withColumn("fuente", F.lit(fuente))
        .withColumn("fecha_procesamiento", F.current_timestamp())
    )
