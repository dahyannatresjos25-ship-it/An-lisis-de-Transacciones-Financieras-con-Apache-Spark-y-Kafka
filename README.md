# An-lisis-de-Transacciones-Financieras-con-Apache-Spark-y-Kafka

**Autor:** Dahyanna Trejos Polanco
**Institución:** Universidad Nacional Abierta y a Distancia (UNAD) - Curso de Big Data  

## Descripción de la Solución
Este proyecto implementa una arquitectura híbrida (Lambda) para el análisis masivo de transacciones financieras, resolviendo el doble desafío del sector bancario: la auditoría de datos históricos y el monitoreo de liquidez en tiempo real. 

La solución se divide en dos capas de procesamiento:
1. **Capa Batch (Spark SQL & DataFrames):** Procesa un conjunto de datos histórico de 100,000 registros sintéticos. Realiza operaciones de limpieza (Data Cleaning) y Análisis Exploratorio de Datos (EDA) para agrupar y calcular el volumen de capital movido por cada tipo de transacción, exportando los resultados a una gráfica estática mediante `matplotlib`.
2. **Capa Streaming (Kafka & Spark Structured Streaming):** Despliega un productor en Python que inyecta eventos transaccionales JSON en vivo a un clúster local de Apache Kafka. Un consumidor desarrollado en PySpark se suscribe al *topic*, parsea los eventos bajo un esquema estricto y aplica operaciones *stateful* con ventanas temporales de 1 minuto para calcular el flujo de caja continuo.

## Archivos del Proyecto
* `generar_dataset.py`: Script para crear el archivo histórico `transacciones_historicas.csv`.
* `spark_batch.py`: Pipeline de procesamiento por lotes para el análisis histórico y graficación.
* `productor_financiero.py`: Simulador de transacciones bancarias enviadas a Kafka.
* `streaming_financiero.py`: Consumidor en tiempo real que procesa los micro-lotes.

## Instrucciones de Ejecución

### Prerrequisitos
* Entorno Linux (Ubuntu).
* Apache Spark 3.x y Apache Kafka 3.x instalados y configurados.
* Python 3 con las librerías `pyspark`, `kafka-python`, `pandas` y `matplotlib` (`pip install pandas matplotlib kafka-python`).

### Paso 1: Levantar la infraestructura de Kafka
Abrir una terminal y ejecutar los servicios en segundo plano (modo daemon):
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties
sudo /opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties

# Para este proyecto de Big Data, estructuré una solución enfocada en el Análisis de Transacciones Financieras. El procesamiento Batch fue desarrollado en PySpark utilizando DataFrames. El código carga un conjunto histórico de 100,000 transacciones financieras. La limpieza de datos (Data Cleaning) se realiza eliminando valores nulos y filtrando transacciones con montos negativos o en cero mediante la función .dropna() y .filter(). El Análisis Exploratorio de Datos (EDA) se ejecuta mediante una función de agregación .groupBy("tipo_trx") que consolida la sumatoria del capital movido y cuenta el número de transacciones. Finalmente, estos datos estructurados se convierten a un dataframe de Pandas para renderizar y exportar un gráfico de barras utilizando matplotlib, permitiendo visualizar visualmente el rendimiento histórico de cada categoría.
# La arquitectura en tiempo real integra Apache Kafka como Message Broker y Spark Structured Streaming como motor de procesamiento. El productor en Python simula el core transaccional de un banco, inyectando eventos JSON al topic transacciones_vivo. El consumidor en PySpark se suscribe a este topic, define un esquema estricto (StructType) para parsear los datos entrantes e implementa operaciones stateful utilizando ventanas de tiempo (window). Específicamente, agrupa las transacciones en intervalos de 1 minuto y calcula la suma continua del capital ingresado por cada tipo de transacción. Los resultados son proyectados en la consola en modo complete, actualizando el flujo de caja de la organización en vivo y en directo.
