from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

spark = SparkSession.builder.appName("StreamingFinanciero").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("trx_id", IntegerType()),
    StructField("tipo_trx", StringType()),
    StructField("monto", FloatType()),
    StructField("timestamp", TimestampType())
])

# Leer de Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transacciones_vivo") \
    .load()

# Parsear JSON
datos_parseados = df.select(from_json(col("value").cast("string"), esquema).alias("data")).select("data.*")

# Procesamiento: Calcular flujo de dinero total por minuto y por tipo de transacción
estadisticas = datos_parseados \
    .groupBy(window(col("timestamp"), "1 minute"), "tipo_trx") \
    .agg(sum("monto").alias("flujo_dinero_minuto"))

# Mostrar en consola
query = estadisticas.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
