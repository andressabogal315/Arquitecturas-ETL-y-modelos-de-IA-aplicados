

from pyspark.sql import functions as F

# ------------------------------------------------------------
# 1. CONFIGURACIÓN
# ------------------------------------------------------------

CATALOG = "aerolinea"
SOURCE_SCHEMA = "fuentes"
VOLUME_NAME = "landing"
BRONZE_SCHEMA = "bronce"

BASE_PATH = (
    f"/Volumes/{CATALOG}/{SOURCE_SCHEMA}/{VOLUME_NAME}"
)

# Archivo de origen -> nombre de tabla Bronze
ARCHIVOS = {
    "vuelos_diarios.csv": "vuelos",
    "aeronaves.csv": "aeronaves",
    "aeropuertos.csv": "aeropuertos",
    "mantenimientos_rds.csv": "mantenimientos",
}

print("=" * 70)
print("INICIO DEL PROCESO BRONZE")
print("=" * 70)
print(f"Catálogo: {CATALOG}")
print(f"Origen: {BASE_PATH}")
print(f"Destino: {CATALOG}.{BRONZE_SCHEMA}")
print()

# ------------------------------------------------------------
# 2. VALIDAR CATÁLOGO Y ESQUEMA DE DESTINO
# ------------------------------------------------------------

spark.sql(f"USE CATALOG `{CATALOG}`")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{BRONZE_SCHEMA}`"
)

print("Catálogo y esquema Bronze verificados.")

# ------------------------------------------------------------
# 3. VALIDAR QUE EXISTAN LOS ARCHIVOS
# ------------------------------------------------------------

try:
    elementos = dbutils.fs.ls(BASE_PATH)
    nombres_disponibles = {
        elemento.name.rstrip("/")
        for elemento in elementos
        if not elemento.isDir()
    }
except Exception as e:
    raise RuntimeError(
        f"No se pudo acceder al volumen de origen: {BASE_PATH}. "
        f"Detalle: {e}"
    ) from e

faltantes = [
    archivo
    for archivo in ARCHIVOS
    if archivo not in nombres_disponibles
]

if faltantes:
    raise FileNotFoundError(
        f"No se encontraron estos archivos en {BASE_PATH}: {faltantes}"
    )

print("Los cuatro archivos CSV están disponibles.")

# ------------------------------------------------------------
# 4. LEER, TRANSFORMAR Y GUARDAR TABLAS DELTA
# ------------------------------------------------------------

resultados = []

for archivo, tabla in ARCHIVOS.items():

    ruta_archivo = f"{BASE_PATH}/{archivo}"
    tabla_destino = f"{CATALOG}.{BRONZE_SCHEMA}.{tabla}"

    print("\n" + "-" * 70)
    print(f"Archivo: {archivo}")
    print(f"Tabla destino: {tabla_destino}")

    try:
        # Leer CSV con encabezados y detección de tipos
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "FAILFAST")
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .load(ruta_archivo)
        )

        # Validar que el archivo tenga columnas
        if not df.columns:
            raise ValueError(
                f"El archivo {archivo} no contiene columnas."
            )

        # Normalizar nombres de columnas:
        # minúsculas, espacios sustituidos por guion bajo
        nombres_nuevos = [
            nombre.strip().lower().replace(" ", "_")
            for nombre in df.columns
        ]

        # Evitar colisiones tras normalizar nombres
        if len(nombres_nuevos) != len(set(nombres_nuevos)):
            raise ValueError(
                f"Hay nombres de columnas duplicados después "
                f"de normalizar {archivo}: {nombres_nuevos}"
            )

        for nombre_original, nombre_nuevo in zip(
            df.columns, nombres_nuevos
        ):
            if nombre_original != nombre_nuevo:
                df = df.withColumnRenamed(
                    nombre_original, nombre_nuevo
                )

        # Añadir marca de tiempo de ingestión
        df = df.withColumn(
            "ingestion_time",
            F.current_timestamp()
        )

        print("\nEsquema de datos:")
        df.printSchema()

        print("\nVista previa:")
        df.show(5, truncate=False)

        # Escribir como Delta.
        # ATENCIÓN: overwrite reemplaza el contenido de la tabla.
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabla_destino)
        )

        # Comprobar que la tabla puede leerse
        df_guardado = spark.table(tabla_destino)
        cantidad = df_guardado.count()

        resultados.append({
            "tabla": tabla_destino,
            "filas": cantidad,
            "estado": "OK",
        })

        print(f"Tabla guardada correctamente: {tabla_destino}")
        print(f"Filas cargadas: {cantidad}")

    except Exception as e:
        print(f"ERROR procesando {archivo}: {e}")
        raise RuntimeError(
            f"Falló la carga de {archivo} hacia {tabla_destino}. "
            f"Error original: {e}"
        ) from e

# ------------------------------------------------------------
# 5. RESUMEN FINAL
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("RESUMEN DE CARGA BRONZE")
print("=" * 70)

for resultado in resultados:
    print(
        f"{resultado['estado']} | "
        f"{resultado['tabla']} | "
        f"Filas: {resultado['filas']}"
    )

print("\nProceso Bronze finalizado correctamente.")
