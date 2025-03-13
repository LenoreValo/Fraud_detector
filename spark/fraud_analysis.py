import findspark
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType, DateType
from pyspark.sql.functions import col,from_json, count, sum, avg, stddev, when
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import os

findspark.init()

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell"

#-------------------------------------------------------------------------------------------------------------------------------------------------------
# Выявление клиентов с аномально большим кол-вом транзакций и аномально большими суммами транзакций 
#-------------------------------------------------------------------------------------------------------------------------------------------------------

# Глобальные переменные
kafka_bootstrap_servers = "172.17.0.13:9092"
kafka_topic = 'e_krylova_transactions_info'
final_output_path = f"D://Aston/AstonProjectB/data/test/csv_files"  # Конечный файл с объединенными данными в CSV

# Создание SparkSession
spark = SparkSession.builder.appName("AnomalyDetection").getOrCreate()

# Вывод ссылки в Spark UI
print("Активные Spark сессии:", spark.sparkContext.uiWebUrl)

spark.conf.set("spark.sql.adaptive.enabled", "false")

# Функция импорта данных из Kafka
def import_table_kafka_to_hdfs(kafka_topic, kafka_bootstrap_servers, schema):
    # Пути для сохранения данных на HDFS clients D://Aston/AstonProjectB/data
    output_path = f"D://Aston/AstonProjectB/data/test/output_raw_json"  # Директория для временных файлов (JSON)
    checkpoint_location = f"D://Aston/AstonProjectB/data/test/checkpoints"  # Директория для checkpoint-файлов


    # Чтение данных из Kafka топик
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "earliest") \
        .load()

    # Извлечение значения (value) из Kafka как строки
    raw_data_df = df.selectExpr("CAST(value AS STRING) as raw_data")

    # Сохранение данных в JSON 
    query = raw_data_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    # Запуск потока с таймаутом (60 секунд)
    print("Начало потока. Остановка через 60 секунд...")
    query.awaitTermination(120)

    if query.isActive:
        print("Таймаут истек. Остановка потока...")
        query.stop()

    # Задаем список выражения для схемы StructType
    schema_list = []
    for key, val in schema.items():
        schema_list.append(StructField(key, val, True))

    if not query.isActive:
        print("Объединение данных в один файл...")

        # Считываем сырые данные из HDFS (в формате JSON)
        try:
            # Попытка автоматического определения схемы
            merged_df = spark.read.json(output_path)
        except Exception as e:
            print(f"Ошибка при автоматическом определении схемы: {e}")
            print("Используем ручное определение схемы...")
            
            # Явно указываем схему для JSON
            json_schema = StructType(schema_list)

            # Читаем данные с использованием явной схемы
            merged_df = spark.read.schema(json_schema).json(output_path)
 
    # Определяем схему для JSON
    json_schema = StructType(schema_list)

    # Парсинг JSON,
    if "raw_data" in merged_df.columns:
        df_parsed = merged_df.withColumn("json_data", from_json(col("raw_data"), json_schema))
        # Выбор всех колонок из json_data
        df_filtered = df_parsed.select("json_data.*")

    # Создаем список выражений для select
    expressions = []
    for key, val in schema.items():
        if 'date' in key or 'birthday' in key:
            expressions.append(col(key).cast(TimestampType()).alias(key))
        elif val == StringType():
            expressions.append(col(key).alias(key))
        else:
            expressions.append(col(key).cast(val).alias(key))
    # Преобразование полей
    df_final = df_filtered.select(*expressions)

    return df_final

#-------------------------------------------------------------------------------------------------------------------------------------------------------
# Вызов функции для импорта данных
schema_transactions = {"client_id": DoubleType(), "transaction_id": DoubleType(), "transaction_date": StringType(), 
          "transaction_type": StringType(), "account_number":  StringType(), 'currency': StringType(),
          "amount": DoubleType(), "record_saved_at": StringType()}
df = import_table_kafka_to_hdfs(kafka_topic, kafka_bootstrap_servers, schema_transactions)
df.show(10, truncate=True)
#-------------------------------------------------------------------------------------------------------------------------------------------------------
# АНАЛИЗ ДАННЫХ НА ФРОД

# 2. Фильтрация и подготовка данных
# Оставляем только релевантные столбцы
df = df.select(
    col("client_id").cast("string").alias("client_id"),
    col("transaction_date").cast("date").alias("transaction_date"),
    col("amount").cast("double").alias("amount")
)
# Удаляем записи с NULL значениями
df = df.dropna(subset=["client_id", "transaction_date", "amount"])
print('Таблица с релевантными значениями без null значений:')
df.show(10, truncate=True)

# 3. Агрегация данных
# Группировка по клиентам и датам для подсчета количества транзакций и сумм
aggregated_df = df.groupBy("client_id", "transaction_date") \
    .agg(
        count("amount").alias("transaction_count"),
        sum("amount").alias("total_transaction_amount")
    )
print('Таблица с руппировкой по клиентам и датам для подсчета количества транзакций и сумм:')
aggregated_df.show(10, truncate=True)

# 4. Вычисление нормальных значений
# Вычисляем среднее и стандартное отклонение для количества транзакций и сумм
window_spec = Window.partitionBy("transaction_date")

aggregated_df = aggregated_df.withColumn("avg_transaction_count", avg("transaction_count").over(window_spec)) \
                             .withColumn("stddev_transaction_count", stddev("transaction_count").over(window_spec)) \
                             .withColumn("avg_total_amount", avg("total_transaction_amount").over(window_spec)) \
                             .withColumn("stddev_total_amount", stddev("total_transaction_amount").over(window_spec))

# Определяем аномальные значения (например, больше чем среднее + 2 * стандартное отклонение)
anomalies_df = aggregated_df.withColumn(
    "is_count_anomaly",
    when(
        (col("transaction_count") > (col("avg_transaction_count") + 2 * col("stddev_transaction_count"))),
        True
    ).otherwise(False)
).withColumn(
    "is_amount_anomaly",
    when(
        (col("total_transaction_amount") > (col("avg_total_amount") + 2 * col("stddev_total_amount"))),
        True
    ).otherwise(False)
)
print('Таблица с аномальными значениями:')
anomalies_df.show(10, truncate=True)

# 5. Фильтрация только аномальных записей
anomalies_df = anomalies_df.filter((col("is_count_anomaly") == True) | (col("is_amount_anomaly") == True))
anomalies_df.show(10, truncate=True)

# 6. Сохранение данных в csv файл
# Сохраняем результат в CSV формате
anomalies_df.write.mode("overwrite").option("header", "true").csv(final_output_path)
print(f"Данные успешно сохранены в {final_output_path}")
    
spark.stop()