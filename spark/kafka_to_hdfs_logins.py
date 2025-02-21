import findspark
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col, from_json
from pyspark.sql import SparkSession
import os

findspark.init()

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell"

# Создание SparkSession
spark = SparkSession.builder \
    .appName("Kafka_To_HDFS") \
    .getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "false")

# Настройки Kafka: топики и сервер Kafka
kafka_topic_logins = "e_krylova_logins_info"
kafka_bootstrap_servers = "172.17.0.13:9092"

#------------------------------------------------------------------------------------------------------------------------------------
# Сохранение файла с информацией о логинах клиентов

# Пути для сохранения данных на HDFS logins
# study_project_b
hdfs_output_path = "hdfs://172.17.0.23:8020/user/e.krylova/test2/logins/output_raw_json"  # Директория для временных файлов (JSON)
checkpoint_location = "hdfs://172.17.0.23:8020/user/e.krylova/test2/logins/checkpoint"  # Директория для checkpoint-файлов
final_output_path = "hdfs://172.17.0.23:8020/user/e.krylova/test2/logins/logins_csv_files"  # Конечный файл с объединенными данными в CSV

# Чтение данных из Kafka топик e_krylova_logins_info
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_logins) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

# Извлечение значения (value) из Kafka как строки
raw_data_df = df.selectExpr("CAST(value AS STRING) as raw_data")

# Сохранение данных в JSON на HDFS
query = raw_data_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", hdfs_output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# 3. Запуск потока с таймаутом (60 секунд)
print("Начало потока. Остановка через 60 секунд...")
query.awaitTermination(120)

if query.isActive:
    print("Таймаут истек. Остановка потока...")
    query.stop()

if not query.isActive:
    print("Объединение данных в один файл...")

    # Считываем сырые данные из HDFS (в формате JSON)
    try:
        # Попытка автоматического определения схемы
        merged_df = spark.read.json(hdfs_output_path)
    except Exception as e:
        print(f"Ошибка при автоматическом определении схемы: {e}")
        print("Используем ручное определение схемы...")
        # Явно указываем схему для JSON
        json_schema = StructType([
            StructField("client_id", DoubleType(), True),
            StructField("login_date", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("location", StringType(), True),
            StructField("device", StringType(), True)
        ])

        # Читаем данные с использованием явной схемы
        merged_df = spark.read.schema(json_schema).json(hdfs_output_path)
    
    print("Объединенный файл:")
    merged_df.show(10, truncate=False)
 
    # Определяем схему для JSON (если необходимо проверить структуру)
    json_schema = StructType([
            StructField("client_id", DoubleType(), True),
            StructField("login_date", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("location", StringType(), True),
            StructField("device", StringType(), True)
        ])

    # Парсинг JSON (если требуется дополнительная обработка)
    if "raw_data" in merged_df.columns:
        df_parsed = merged_df.withColumn("json_data", from_json(col("raw_data"), json_schema))
        df_filtered = df_parsed.select("json_data.*").filter(col("client_id").isNotNull())
    else:
        df_filtered = merged_df.filter(col("client_id").isNotNull())


    # Преобразование полей
    df_final = df_filtered.select(
        col("client_id").cast(DoubleType()).alias("client_id"),
        col("login_date").cast(TimestampType()).alias("login_date"),
        col("ip_address").alias("ip_address"),
        col("location").alias("location"),
        col("device").alias("device")
    )

    # Проверяем результат
    print("Результативный датафрейм:")
    df_final.show(10, truncate=False)

    # Сохраняем результат в CSV формате
    df_final.write.mode("overwrite").option("header", "true").csv(final_output_path)

    print(f"Данные успешно сохранены в {final_output_path}")
    

