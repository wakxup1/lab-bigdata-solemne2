from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, min, max, count, when
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder \
    .appName("Solemne02-RidesharingAnalysis") \
    .getOrCreate()

print("--- 1. CARGA DE DATOS ---")
df_raw = spark.read.json("viajes_raw.jsonl")

print("Esquema inicial:")
df_raw.printSchema()
conteo_inicial = df_raw.count()
print(f"Registros totales antes de limpieza: {conteo_inicial}")

print("\n--- 2. PREPARACIÓN Y TRATAMIENTO DE CALIDAD ---")
df_prep = df_raw.withColumn("marca_temporal", to_timestamp(col("marca_temporal"), "yyyy-MM-dd HH:mm:ss"))

df_limpio = df_prep.withColumn(
    "tarifa", 
    when((col("tarifa").isNull()) | (col("tarifa") < 0), None).otherwise(col("tarifa"))
)

df_analisis = df_limpio.dropna(subset=["tarifa"])
conteo_final = df_analisis.count()

print(f"Registros después de tratar la anomalía: {conteo_final}")
print(f"Registros descartados por anomalía: {conteo_inicial - conteo_final}")

print("\n--- 3. ANÁLISIS EXPLORATORIO ---")

print("\n[Pregunta 1] Estadística descriptiva de la tarifa:")
df_analisis.select(
    avg("tarifa").alias("tarifa_promedio"),
    min("tarifa").alias("tarifa_minima"),
    max("tarifa").alias("tarifa_maxima")
).show()

print("\n[Pregunta 2] Comparación por tipo de servicio:")
df_cat = df_analisis.groupBy("tipo_servicio").agg(
    count("id_evento").alias("total_viajes"),
    avg("tarifa").alias("tarifa_promedio")
).orderBy(col("total_viajes").desc())
df_cat.show()

print("\n[Pregunta 3] Evolución de viajes por hora del día:")
df_tiempo = df_analisis.groupBy("hora_dia").agg(
    count("id_evento").alias("total_viajes")
).orderBy("hora_dia")
df_tiempo.show()

print("\n--- 4. GENERACIÓN DE LAS 3 VISUALIZACIONES ---")
pdf_cat = df_cat.toPandas()
pdf_tiempo = df_tiempo.toPandas()
pdf_tarifa = df_analisis.select("tarifa").toPandas()

# Gráfico 1: Descriptiva (Histograma de Tarifas)
plt.figure(figsize=(8, 5))
plt.hist(pdf_tarifa["tarifa"], bins=20, color='skyblue', edgecolor='black')
plt.title("Distribución de Tarifas de Viajes (Pregunta 1)")
plt.xlabel("Tarifa (CLP)")
plt.ylabel("Frecuencia")
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig("grafico_1_descriptiva.png")
plt.close()

# Gráfico 2: Comparación por Categoría (Tipo de Servicio)
plt.figure(figsize=(8, 5))
plt.bar(pdf_cat["tipo_servicio"], pdf_cat["tarifa_promedio"], color=['#ff9999','#66b3ff','#99ff99'])
plt.title("Tarifa Promedio por Tipo de Servicio (Pregunta 2)")
plt.xlabel("Tipo de Servicio")
plt.ylabel("Tarifa Promedio (CLP)")
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig("grafico_2_categoria.png")
plt.close()

# Gráfico 3: Evolución Temporal (Viajes por Hora del Día)
plt.figure(figsize=(8, 5))
plt.plot(pdf_tiempo["hora_dia"], pdf_tiempo["total_viajes"], marker='o', color='#2b6cb0', linewidth=2)
plt.title("Evolución de Viajes según Hora del Día (Pregunta 3)")
plt.xlabel("Hora del Día (0-23)")
plt.ylabel("Total de Viajes")
plt.grid(True, linestyle='--', alpha=0.7)
plt.savefig("grafico_3_temporal.png")
plt.close()

print("¡Los 3 gráficos (grafico_1, grafico_2 y grafico_3) generados y guardados exitosamente como PNG!")
spark.stop()