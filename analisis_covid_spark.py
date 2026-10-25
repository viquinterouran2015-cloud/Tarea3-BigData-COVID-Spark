# =====================================================================
# Tarea 3 - Big Data - Procesamiento en batch con Apache Spark
# Problema: identificar qué departamentos, grupos de edad y sexos
# concentraron más casos y muertes por COVID-19 en Colombia.
# Fuente: Casos positivos de COVID-19 en Colombia, datos.gov.co, id gt2j-8ykr
# Autora: Laura Victoria Quintero Urán
# Recomendado: ejecutar en Google Colab, celda por celda.
# =====================================================================

# --- 1. Instalar PySpark (solo en Colab) -----------------------------
# !pip install pyspark

# --- 2. Descargar el conjunto de datos -------------------------------
# El archivo completo pesa varios GB. Para la tarea basta con una
# muestra grande: aquí se descargan 1.000.000 de registros desde la API.
# !wget -O covid.csv 'https://www.datos.gov.co/resource/gt2j-8ykr.csv?$limit=1000000'

import unicodedata
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- 3. Crear la sesión de Spark -------------------------------------
spark = SparkSession.builder.appName("COVID19_Colombia_Batch").getOrCreate()

# --- 4. Cargar los datos ---------------------------------------------
df = spark.read.csv("covid.csv", header=True, inferSchema=True)
print("Registros cargados:", df.count())


# --- 5. Normalizar nombres de columnas -------------------------------
# Se quitan tildes, espacios y mayúsculas para trabajar más fácil.
def normalizar(nombre):
    sin_tildes = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return sin_tildes.strip().lower().replace(" ", "_")

df = df.toDF(*[normalizar(c) for c in df.columns])
print(df.columns)

# La API usa estos nombres; si descargaste el CSV desde la página web,
# revisa el print anterior y ajusta los nombres en este diccionario.
COL = {
    "id": "id_de_caso",
    "depto": "departamento_nom",
    "edad": "edad",
    "sexo": "sexo",
    "estado": "estado",
    "recuperado": "recuperado",
    "fecha_muerte": "fecha_muerte",
}

# --- 6. Limpieza -----------------------------------------------------
datos = (
    df.select(*[F.col(v).alias(k) for k, v in COL.items()])
      # Quitar registros sin departamento o sin edad
      .dropna(subset=["depto", "edad"])
      # Estandarizar texto
      .withColumn("depto", F.upper(F.trim(F.col("depto"))))
      .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
      .withColumn("recuperado", F.lower(F.trim(F.col("recuperado"))))
      # Edad como número y sin valores imposibles
      .withColumn("edad", F.col("edad").cast("int"))
      .filter((F.col("edad") >= 0) & (F.col("edad") <= 110))
      # Quitar duplicados según el identificador del caso
      .dropDuplicates(["id"])
)

# --- 7. Transformación -----------------------------------------------
# Crear grupos de edad y una marca de fallecimiento
datos = (
    datos.withColumn(
        "grupo_edad",
        F.when(F.col("edad") < 20, "0-19")
         .when(F.col("edad") < 40, "20-39")
         .when(F.col("edad") < 60, "40-59")
         .when(F.col("edad") < 80, "60-79")
         .otherwise("80+"),
    )
    .withColumn(
        "fallecido",
        F.when(
            (F.col("recuperado") == "fallecido") | F.col("fecha_muerte").isNotNull(), 1
        ).otherwise(0),
    )
)
datos.cache()
print("Registros después de limpiar:", datos.count())

# --- 8. Análisis exploratorio ----------------------------------------
print("Estadísticas de la edad:")
datos.select("edad").describe().show()

# 8.1 Top 10 departamentos con más casos
top_deptos = (
    datos.groupBy("depto").count()
         .orderBy(F.desc("count")).limit(10)
)
top_deptos.show()

# 8.2 Casos y muertes por grupo de edad, con tasa de letalidad
por_edad = (
    datos.groupBy("grupo_edad")
         .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("muertes"))
         .withColumn("letalidad_pct", F.round(F.col("muertes") / F.col("casos") * 100, 2))
         .orderBy("grupo_edad")
)
por_edad.show()

# 8.3 Casos por sexo
por_sexo = datos.groupBy("sexo").count().orderBy(F.desc("count"))
por_sexo.show()

# 8.4 Ejemplo con RDD: contar casos por departamento con map y reduceByKey
rdd_conteo = (
    datos.select("depto").rdd
         .map(lambda fila: (fila["depto"], 1))
         .reduceByKey(lambda a, b: a + b)
         .takeOrdered(5, key=lambda x: -x[1])
)
print("Top 5 con RDD:", rdd_conteo)

# --- 9. Visualización ------------------------------------------------
pd_deptos = top_deptos.toPandas()
pd_edad = por_edad.toPandas()
pd_sexo = por_sexo.toPandas()

plt.figure(figsize=(10, 5))
plt.barh(pd_deptos["depto"], pd_deptos["count"])
plt.gca().invert_yaxis()
plt.title("Top 10 departamentos con más casos de COVID-19")
plt.xlabel("Casos")
plt.tight_layout()
plt.show()

plt.figure(figsize=(8, 5))
plt.bar(pd_edad["grupo_edad"], pd_edad["letalidad_pct"])
plt.title("Letalidad por grupo de edad")
plt.ylabel("% de fallecidos")
plt.tight_layout()
plt.show()

plt.figure(figsize=(5, 5))
plt.pie(pd_sexo["count"], labels=pd_sexo["sexo"], autopct="%1.1f%%")
plt.title("Distribución de casos por sexo")
plt.show()

spark.stop()
