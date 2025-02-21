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
kafka_topic_clients = "e_krylova_clients_info"
kafka_topic_activity = "e_krylova_client_activity_info"
kafka_topic_logins = "e_krylova_logins_info"
kafka_topic_payments = "e_krylova_payments_info"
kafka_topic_transactions = "e_krylova_transactions_info"
kafka_bootstrap_servers = "172.17.0.13:9092"

#------------------------------------------------------------------------------------------------------------------------------------
# Сохранение файла с информацией о клиентах

# Пути для сохранения данных на HDFS clients
# study_project_b
hdfs_output_path = "hdfs://172.17.0.23:8020/user/e.krylova/test2/clients/output_raw_json"  # Директория для временных файлов (JSON)
checkpoint_location = "hdfs://172.17.0.23:8020/user/e.krylova/test2/clients/checkpoint"  # Директория для checkpoint-файлов
final_output_path = "hdfs://172.17.0.23:8020/user/e.krylova/test2/clients/clients_csv_files"  # Конечный файл с объединенными данными в CSV

# Чтение данных из Kafka топик e_krylova_clients_info
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_clients) \
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
            StructField("client_first_name", StringType(), True),
            StructField("client_last_name", StringType(), True),
            StructField("client_email", StringType(), True),
            StructField("client_phone", StringType(), True),
            StructField("client_address", StringType(), True),
            StructField("client_birthday", StringType(), True)
        ])

        # Читаем данные с использованием явной схемы
        merged_df = spark.read.schema(json_schema).json(hdfs_output_path)
    
    print("Объединенный файл:")
    merged_df.show(10, truncate=False)
 
    # Определяем схему для JSON (если необходимо проверить структуру)
    json_schema = StructType([
            StructField("client_id", DoubleType(), True),
            StructField("client_first_name", StringType(), True),
            StructField("client_last_name", StringType(), True),
            StructField("client_email", StringType(), True),
            StructField("client_phone", StringType(), True),
            StructField("client_address", StringType(), True),
            StructField("client_birthday", StringType(), True)
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
        col("client_first_name").alias("client_first_name"),
        col("client_last_name").alias("client_last_name"),
        col("client_email").alias("client_email"),
        col("client_phone").alias("client_phone"),
        col("client_address").alias("client_address"),
        col("client_birthday").cast(TimestampType()).alias("client_birthday")
    )

    # Проверяем результат
    print("Результативный датафрейм:")
    df_final.show(10, truncate=False)

    # Сохраняем результат в CSV формате
    df_final.write.mode("overwrite").option("header", "true").csv(final_output_path)

    print(f"Данные успешно сохранены в {final_output_path}")
    


#------------------------------------------------------------------------------------------------------------------------------------
# Сохранение файла с информацией об логинах клиентов
