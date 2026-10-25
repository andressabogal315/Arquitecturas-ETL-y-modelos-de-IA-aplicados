# 03_gold_analytics.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# Configuración
# =========================
CATALOG = "airline_exam"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# =========================
# 1. Leer Silver
# =========================
vuelos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")