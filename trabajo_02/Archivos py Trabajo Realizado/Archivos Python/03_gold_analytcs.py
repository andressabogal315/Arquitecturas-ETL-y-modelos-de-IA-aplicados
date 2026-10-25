
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# 1. INICIALIZAR SPARK Y CONFIGURACIÓN
# ============================================================

spark = SparkSession.builder \
    .appName("AerolineaGold") \
    .getOrCreate()

CATALOG = "aerolinea"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

print("=" * 70)
print("INICIO DEL PROCESO GOLD")
print("=" * 70)

# ============================================================
# 2. ASEGURAR ESQUEMA GOLD
# ============================================================

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{GOLD_SCHEMA}`"
)

# ============================================================
# 3. LEER TABLAS SILVER
# ============================================================

v = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")
aeronaves = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")
aeropuertos = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")
m = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

# Comprobar columnas requeridas
columnas_requeridas = {
    "vuelos": [
        "vuelo_id", "fecha", "origen_id", "destino_id",
        "aeronave_id", "estado", "duracion_min"
    ],
    "aeronaves": [
        "aeronave_id", "modelo", "fabricante", "anio_fabricacion"
    ],
    "aeropuertos": [
        "aeropuerto_id", "nombre", "ciudad", "pais"
    ],
    "mantenimientos": [
        "mantenimiento_id", "aeronave_id", "fecha",
        "tipo", "costo_usd", "duracion_hr"
    ]
}

dataframes = {
    "vuelos": v,
    "aeronaves": aeronaves,
    "aeropuertos": aeropuertos,
    "mantenimientos": m
}

for nombre, columnas in columnas_requeridas.items():
    faltantes = set(columnas) - set(dataframes[nombre].columns)

    if faltantes:
        raise ValueError(
            f"Faltan columnas en Silver {nombre}: {sorted(faltantes)}"
        )

print("Tablas Silver y columnas verificadas.")

# ============================================================
# 4. VUELOS ENRIQUECIDOS
# ============================================================

# Normalizar estado para que las comparaciones no dependan
# de mayúsculas o espacios.
v = v.withColumn(
    "estado",
    F.lower(F.trim(F.col("estado")))
).alias("v")

a = aeronaves.alias("a")
o = aeropuertos.alias("o")
d = aeropuertos.alias("d")

# Compatibilidad con tablas Silver que no tengan esta columna.
if "ruta_por_revisar" in v.columns:
    ruta_revision = F.coalesce(
        F.col("v.ruta_por_revisar").cast("boolean"),
        F.lit(False)
    )
else:
    ruta_revision = F.lit(False)

vuelos_enriquecidos = (
    v
    .join(
        a,
        F.col("v.aeronave_id") == F.col("a.aeronave_id"),
        "left"
    )
    .join(
        o,
        F.col("v.origen_id") == F.col("o.aeropuerto_id"),
        "left"
    )
    .join(
        d,
        F.col("v.destino_id") == F.col("d.aeropuerto_id"),
        "left"
    )
    .select(
        F.col("v.vuelo_id").alias("vuelo_id"),
        F.col("v.fecha").alias("fecha"),
        F.trunc(F.col("v.fecha"), "month").alias("mes"),
        F.col("v.aeronave_id").alias("aeronave_id"),
        F.col("a.modelo").alias("modelo"),
        F.col("a.fabricante").alias("fabricante"),
        F.col("a.anio_fabricacion").alias("anio_fabricacion"),
        (
            F.year(F.col("v.fecha")) -
            F.col("a.anio_fabricacion")
        ).alias("edad_aeronave"),
        F.col("v.origen_id").alias("origen_id"),
        F.col("o.nombre").alias("origen_nombre"),
        F.col("o.ciudad").alias("origen_ciudad"),
        F.col("o.pais").alias("origen_pais"),
        F.col("v.destino_id").alias("destino_id"),
        F.col("d.nombre").alias("destino_nombre"),
        F.col("d.ciudad").alias("destino_ciudad"),
        F.col("d.pais").alias("destino_pais"),
        F.col("v.estado").alias("estado"),
        F.col("v.duracion_min").alias("duracion_min"),
        (
            F.col("v.estado").isNotNull() &
            (F.col("v.estado") != "cancelado")
        ).alias("operado"),
        ruta_revision.alias("ruta_por_revisar")
    )
)

# Validar que los joins no multipliquen registros.
vuelos_silver_count = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.vuelos"
).count()

vuelos_gold_count = vuelos_enriquecidos.count()

if vuelos_gold_count != vuelos_silver_count:
    raise ValueError(
        "El enriquecimiento multiplicó o perdió vuelos. "
        f"Silver={vuelos_silver_count}, Gold={vuelos_gold_count}. "
        "Verifica que aeronave_id y aeropuerto_id sean únicos "
        "en sus tablas de referencia."
    )

(
    vuelos_enriquecidos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos")
)

print(f"Vuelos enriquecidos guardados: {vuelos_gold_count}")

# ============================================================
# 5. MANTENIMIENTOS ENRIQUECIDOS
# ============================================================

a = aeronaves.alias("a")
m = m.alias("m")

mantenimientos_enriquecidos = (
    m
    .join(
        a,
        F.col("m.aeronave_id") == F.col("a.aeronave_id"),
        "left"
    )
    .select(
        F.col("m.mantenimiento_id").alias("mantenimiento_id"),
        F.col("m.fecha").alias("fecha"),
        F.trunc(F.col("m.fecha"), "month").alias("mes"),
        F.col("m.aeronave_id").alias("aeronave_id"),
        F.col("a.modelo").alias("modelo"),
        F.col("a.fabricante").alias("fabricante"),
        F.col("a.anio_fabricacion").alias("anio_fabricacion"),
        (
            F.year(F.col("m.fecha")) -
            F.col("a.anio_fabricacion")
        ).alias("edad_aeronave"),
        F.col("m.tipo").alias("tipo"),
        F.col("m.costo_usd").alias("costo_usd"),
        F.col("m.duracion_hr").alias("duracion_hr")
    )
)

mantenimientos_silver_count = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos"
).count()

mantenimientos_gold_count = mantenimientos_enriquecidos.count()

if mantenimientos_gold_count != mantenimientos_silver_count:
    raise ValueError(
        "El enriquecimiento multiplicó o perdió mantenimientos. "
        f"Silver={mantenimientos_silver_count}, "
        f"Gold={mantenimientos_gold_count}. "
        "Verifica la unicidad de aeronave_id en Silver."
    )

(
    mantenimientos_enriquecidos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.mantenimientos_enriquecidos"
    )
)

print(
    "Mantenimientos enriquecidos guardados: "
    f"{mantenimientos_gold_count}"
)

# ============================================================
# 6. KPI DE PUNTUALIDAD DE VUELOS
# ============================================================

ve = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos"
)

kpi_puntualidad = (
    ve
    .groupBy("modelo", "fabricante", "origen_pais", "mes")
    .agg(
        F.count("*").alias("total_vuelos"),

        F.sum(
            F.when(F.col("operado"), 1).otherwise(0)
        ).alias("vuelos_operados"),

        F.sum(
            F.when(F.col("estado") == "a_tiempo", 1).otherwise(0)
        ).alias("vuelos_a_tiempo"),

        F.sum(
            F.when(F.col("estado") == "retrasado", 1).otherwise(0)
        ).alias("vuelos_retrasados"),

        F.sum(
            F.when(F.col("estado") == "cancelado", 1).otherwise(0)
        ).alias("vuelos_cancelados"),

        F.sum(
            F.when(F.col("ruta_por_revisar"), 1).otherwise(0)
        ).alias("rutas_por_revisar"),

        F.avg(
            F.when(F.col("operado"), F.col("duracion_min"))
        ).alias("_duracion_promedio"),

        F.sum(
            F.when(F.col("operado"), 1).otherwise(0)
        ).alias("_denominador_operados")
    )
    .withColumn(
        "puntualidad_operados_pct",
        F.when(
            F.col("_denominador_operados") > 0,
            F.round(
                F.lit(100.0) * F.col("vuelos_a_tiempo") /
                F.col("_denominador_operados"),
                2
            )
        )
    )
    .withColumn(
        "cancelacion_pct",
        F.round(
            F.lit(100.0) * F.col("vuelos_cancelados") /
            F.col("total_vuelos"),
            2
        )
    )
    .withColumn(
        "duracion_promedio_operados_min",
        F.round(F.col("_duracion_promedio"), 2)
    )
    .drop("_duracion_promedio", "_denominador_operados")
)

(
    kpi_puntualidad.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos"
    )
)

print("KPI de puntualidad creados.")

# ============================================================
# 7. RESUMEN GLOBAL DE PUNTUALIDAD
# ============================================================

kp = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos"
)

resumen_puntualidad = kp.agg(
    F.sum("total_vuelos").alias("vuelos"),
    F.sum("vuelos_operados").alias("operados"),
    F.sum("vuelos_a_tiempo").alias("a_tiempo"),
    F.sum("vuelos_retrasados").alias("retrasados"),
    F.sum("vuelos_cancelados").alias("cancelados"),
    F.round(
        F.lit(100.0) * F.sum("vuelos_a_tiempo") /
        F.when(
            F.sum("vuelos_operados") > 0,
            F.sum("vuelos_operados")
        ),
        2
    ).alias("puntualidad_pct"),
    F.round(
        F.lit(100.0) * F.sum("vuelos_cancelados") /
        F.when(F.sum("total_vuelos") > 0, F.sum("total_vuelos")),
        2
    ).alias("cancelacion_pct")
)

print("Resumen global de puntualidad:")
resumen_puntualidad.show(truncate=False)

# ============================================================
# 8. KPI DE COSTOS DE MANTENIMIENTO
# ============================================================

me = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.mantenimientos_enriquecidos"
)

kpi_mantenimiento = (
    me
    .groupBy("aeronave_id", "modelo", "fabricante", "tipo", "mes")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.sum("duracion_hr").alias("duracion_total_hr"),
        F.round(F.avg("duracion_hr"), 2).alias("duracion_promedio_hr")
    )
)

(
    kpi_mantenimiento.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento"
    )
)

print("KPI de costos de mantenimiento creados.")

# ============================================================
# 9. RESUMEN DE COSTOS POR TIPO
# ============================================================

resumen_mantenimiento = (
    spark.table(
        f"{CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento"
    )
    .groupBy("tipo")
    .agg(
        F.sum("total_mantenimientos").alias("intervenciones"),
        F.sum("costo_total_usd").alias("costo_total_usd"),
        F.round(F.avg("costo_promedio_usd"), 2)
        .alias("costo_promedio_usd")
    )
    .orderBy(F.col("costo_total_usd").desc_nulls_last())
)

print("Resumen de costos por tipo de mantenimiento:")
resumen_mantenimiento.show(truncate=False)

# ============================================================
# 10. RESUMEN MENSUAL POR AERONAVE
# ============================================================

actividad = (
    ve
    .groupBy("aeronave_id", "mes")
    .agg(
        F.count("*").alias("total_vuelos"),
        F.sum(
            F.when(F.col("operado"), 1).otherwise(0)
        ).alias("vuelos_operados")
    )
)

mantenimiento = (
    me
    .groupBy("aeronave_id", "mes")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd")
    )
)

# Full outer conserva los meses que solo aparecen en vuelos
# o solo aparecen en mantenimientos.
resumen_aeronave_mes = (
    actividad.alias("v")
    .join(
        mantenimiento.alias("m"),
        (
            F.col("v.aeronave_id") == F.col("m.aeronave_id")
        ) & (
            F.col("v.mes") == F.col("m.mes")
        ),
        "full_outer"
    )
    .select(
        F.coalesce(
            F.col("v.aeronave_id"),
            F.col("m.aeronave_id")
        ).alias("aeronave_id"),

        F.coalesce(
            F.col("v.mes"),
            F.col("m.mes")
        ).alias("mes"),

        F.coalesce(
            F.col("v.total_vuelos"), F.lit(0)
        ).alias("total_vuelos"),

        F.coalesce(
            F.col("v.vuelos_operados"), F.lit(0)
        ).alias("vuelos_operados"),

        F.coalesce(
            F.col("m.total_mantenimientos"), F.lit(0)
        ).alias("total_mantenimientos"),

        F.coalesce(
            F.col("m.costo_total_usd"), F.lit(0.0)
        ).alias("costo_total_usd")
    )
)

(
    resumen_aeronave_mes.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.resumen_aeronave_mes"
    )
)

print("Resumen mensual por aeronave creado.")

# ============================================================
# 11. BASE PARA MACHINE LEARNING
# ============================================================

base_ml = me.select(
    "mantenimiento_id",
    "fecha",
    "aeronave_id",
    "tipo",
    "modelo",
    "fabricante",
    "edad_aeronave",
    "costo_usd"
)

(
    base_ml.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.base_ml_mantenimiento"
    )
)

print("Base para Machine Learning creada.")

# ============================================================
# 12. VALIDACIÓN FINAL DE TABLAS GOLD
# ============================================================

tablas_gold = [
    "vuelos_enriquecidos",
    "mantenimientos_enriquecidos",
    "kpi_puntualidad_vuelos",
    "kpi_costo_mantenimiento",
    "resumen_aeronave_mes",
    "base_ml_mantenimiento"
]

print("\n" + "=" * 70)
print("VALIDACIÓN FINAL DE GOLD")
print("=" * 70)

for tabla in tablas_gold:
    nombre_completo = f"{CATALOG}.{GOLD_SCHEMA}.{tabla}"
    cantidad = spark.table(nombre_completo).count()
    print(f"{nombre_completo}: {cantidad} registros")

print("\nProceso Gold finalizado correctamente.")
