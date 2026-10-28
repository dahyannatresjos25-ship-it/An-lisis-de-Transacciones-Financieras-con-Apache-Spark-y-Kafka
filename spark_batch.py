from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round
import matplotlib.pyplot as plt
import pandas as pd

# 1. Inicializar Spark
spark = SparkSession.builder.appName("BatchAnalisisFinanciero").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Cargar datos
df = spark.read.csv("transacciones_historicas.csv", header=True, inferSchema=True)

# 3. Limpieza y Transformación
df_clean = df.dropna().filter(col("monto") > 0)

# 4. Análisis Exploratorio (EDA): Agrupar por tipo y sumar montos
resumen_df = df_clean.groupBy("tipo_trx").agg(
    count("trx_id").alias("cantidad_operaciones"),
    round(sum("monto"), 2).alias("volumen_total_dinero")
).orderBy(col("volumen_total_dinero").desc())

print("--- RESULTADOS DEL PROCESAMIENTO BATCH ---")
resumen_df.show()

# 5. Visualización: Convertir a Pandas y graficar
pandas_df = resumen_df.toPandas()
plt.figure(figsize=(10, 6))
plt.bar(pandas_df['tipo_trx'], pandas_df['volumen_total_dinero'], color=['blue', 'green', 'orange', 'red'])
plt.title('Volumen Total de Dinero por Tipo de Transacción')
plt.xlabel('Tipo de Transacción')
plt.ylabel('Monto Total ($)')
plt.xticks(rotation=15)
plt.tight_layout()
plt.savefig('grafica_batch.png')
print("Gráfica guardada como 'grafica_batch.png'")

spark.stop()
