
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURACIÓN
# ============================================================

CATALOG = "aerolinea"
BRONZE_SCHEMA = "bronce"
SILVER_SCHEMA = "silver"

spark = SparkSession.builder.getOrCreate()

print("=" * 65)
print("INICIO DEL PROCESO SILVER")
print("=" * 65)

# ============================================================
# 0. CREAR ESQUEMA SILVER SI NO EXISTE
# ============================================================

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SILVER_SCHEMA}`"
)

print(f"Esquema verificado: {CATALOG}.{SILVER_SCHEMA}")

# ============================================================
# 1. LEER TABLAS BRONZE
# ============================================================

vuelos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vuelos")
aeronaves_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeronaves")
aeropuertos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeropuertos")
mantenimientos_bz = spark.table(
    f"{CATALOG}.{BRONZE_SCHEMA}.mantenimientos"
)

print("Tablas Bronze leídas correctamente.")

# ============================================================
# 2. TRANSFORMACIONES SILVER
# ============================================================

# -------------------------
# 2.1 VUELOS
# -------------------------

vuelos_slv = (
    vuelos_bz
    .select(
        "vuelo_id",
        "fecha",
        "origen_id",
        "destino_id",
        "aeronave_id",
        "estado",
        "duracion_min"
    )
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn(
        "vuelo_id",
        F.trim(F.col("vuelo_id").cast("string"))
    )
    .withColumn(
        "origen_id",
        F.trim(F.col("origen_id").cast("string"))
    )
    .withColumn(
        "destino_id",
        F.trim(F.col("destino_id").cast("string"))
    )
    .withColumn(
        "aeronave_id",
        F.trim(F.col("aeronave_id").cast("string"))
    )
    .withColumn("estado", F.upper(F.trim(F.col("estado"))))
    .withColumn("duracion_min", F.col("duracion_min").cast("int"))
    .dropDuplicates(["vuelo_id"])
)

# -------------------------
# 2.2 AERONAVES
# -------------------------

aeronaves_slv = (
    aeronaves_bz
    .select(
        "aeronave_id",
        "modelo",
        "fabricante",
        "anio_fabricacion"
    )
    .withColumn(
        "aeronave_id",
        F.trim(F.col("aeronave_id").cast("string"))
    )
    .withColumn("modelo", F.trim(F.col("modelo")))
    .withColumn("fabricante", F.trim(F.col("fabricante")))
    .withColumn(
        "anio_fabricacion",
        F.col("anio_fabricacion").cast("int")
    )
    .dropDuplicates(["aeronave_id"])
)

# -------------------------
# 2.3 AEROPUERTOS
# -------------------------

aeropuertos_slv = (
    aeropuertos_bz
    .select(
        "aeropuerto_id",
        "nombre",
        "ciudad",
        "pais",
        "lat",
        "lon"
    )
    .withColumn(
        "aeropuerto_id",
        F.trim(F.col("aeropuerto_id").cast("string"))
    )
    .withColumn("nombre", F.trim(F.col("nombre")))
    .withColumn("ciudad", F.trim(F.col("ciudad")))
    .withColumn("pais", F.trim(F.col("pais")))
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .dropDuplicates(["aeropuerto_id"])
)

# -------------------------
# 2.4 MANTENIMIENTOS
# -------------------------

mantenimientos_slv = (
    mantenimientos_bz
    .select(
        "mantenimiento_id",
        "aeronave_id",
        "fecha",
        "tipo",
        "costo_usd",
        "duracion_hr"
    )
    .withColumn(
        "mantenimiento_id",
        F.trim(F.col("mantenimiento_id").cast("string"))
    )
    .withColumn(
        "aeronave_id",
        F.trim(F.col("aeronave_id").cast("string"))
    )
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("tipo", F.trim(F.col("tipo")))
    .withColumn("costo_usd", F.col("costo_usd").cast("double"))
    .withColumn("duracion_hr", F.col("duracion_hr").cast("double"))
    .dropDuplicates(["mantenimiento_id"])
)

print("Transformaciones Silver completadas.")

# ============================================================
# 3. VALIDACIONES DE CALIDAD
# ============================================================

def validar_calidad(df, nombre_tabla, clave):
    print(f"\n--- Validación: {nombre_tabla} ---")

    total = df.count()

    nulos_clave = df.filter(
        F.col(clave).isNull()
        | (F.trim(F.col(clave).cast("string")) == "")
    ).count()

    duplicados_clave = (
        df.filter(F.col(clave).isNotNull())
        .groupBy(clave)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Total de registros: {total}")
    print(f"Claves nulas o vacías: {nulos_clave}")
    print(f"Claves duplicadas: {duplicados_clave}")

    return {
        "tabla": nombre_tabla,
        "registros": total,
        "claves_nulas": nulos_clave,
        "claves_duplicadas": duplicados_clave
    }


resultados_calidad = []

resultados_calidad.append(
    validar_calidad(vuelos_slv, "vuelos", "vuelo_id")
)

resultados_calidad.append(
    validar_calidad(aeronaves_slv, "aeronaves", "aeronave_id")
)

resultados_calidad.append(
    validar_calidad(aeropuertos_slv, "aeropuertos", "aeropuerto_id")
)

resultados_calidad.append(
    validar_calidad(
        mantenimientos_slv,
        "mantenimientos",
        "mantenimiento_id"
    )
)

# ============================================================
# 4. GUARDAR TABLAS SILVER EN DELTA
# ============================================================

tablas_silver = {
    "vuelos": vuelos_slv,
    "aeronaves": aeronaves_slv,
    "aeropuertos": aeropuertos_slv,
    "mantenimientos": mantenimientos_slv
}

for nombre, df in tablas_silver.items():
    tabla_destino = f"{CATALOG}.{SILVER_SCHEMA}.{nombre}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino)
    )

    print(f"Tabla guardada correctamente: {tabla_destino}")

# ============================================================
# 5. RESUMEN FINAL
# ============================================================

print("\n" + "=" * 65)
print("RESUMEN FINAL DE SILVER")
print("=" * 65)

for nombre in tablas_silver:
    tabla_destino = f"{CATALOG}.{SILVER_SCHEMA}.{nombre}"
    total = spark.table(tabla_destino).count()
    print(f"{tabla_destino}: {total} registros")

print("\nProceso Silver finalizado correctamente.")
