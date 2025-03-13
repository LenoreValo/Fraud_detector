#import findspark
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
from pyspark.sql.functions import col, from_json
from pyspark.sql import SparkSession
import os

#-------------------------------------------------------------------------------------------------------------------
# ЗАГРУЗКА ДАННЫХ ИЗ KAFKA В HDFS 

#findspark.init()

#os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell"

# Глобальные переменные
kafka_bootstrap_servers = "172.17.0.13:9092"
hdfs_server = '172.17.0.23:8020'
user_hdfs = 'e.krylova'
folder_name_global = 'study_project_b'
#---------------------------------------------------------------------------------------------------------------------------------------------------------------
# Функция импорта данных из Kafka в HDFS
def import_table_kafka_to_hdfs(hdfs_server, user_hdfs, kafka_topic, kafka_bootstrap_servers, folder_name, schema):

    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("Kafka_to_HDFS") \
        .getOrCreate()

    # Вывод ссылки в Spark UI
    print("Активные Spark сессии:", spark.sparkContext.uiWebUrl)

    #spark.conf.set("spark.sql.adaptive.enabled", "false")

    # Пути для сохранения данных на HDFS clients
    hdfs_output_path = f"hdfs://{hdfs_server}/user/{user_hdfs}/{folder_name_global}/{folder_name}/output_raw_json"  # Директория для временных файлов (JSON)
    checkpoint_location = f"hdfs://{hdfs_server}/user/{user_hdfs}/{folder_name_global}/{folder_name}/checkpoints"  # Директория для checkpoint-файлов
    final_output_path = f"hdfs://{hdfs_server}/user/{user_hdfs}/{folder_name_global}/{folder_name}/csv_files"  # Конечный файл с объединенными данными в CSV

    try:
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

        # Сохранение данных в JSON на HDFS
        query = raw_data_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", hdfs_output_path) \
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
                merged_df = spark.read.json(hdfs_output_path)
            except Exception as e:
                print(f"Ошибка при автоматическом определении схемы: {e}")
                print("Используем ручное определение схемы...")
                
                # Явно указываем схему для JSON
                json_schema = StructType(schema_list)

                # Читаем данные с использованием явной схемы
                merged_df = spark.read.schema(json_schema).json(hdfs_output_path)
        
        print("Объединенный файл:")
        merged_df.show(10, truncate=False)
    
        # Определяем схему для JSON
        json_schema = StructType(schema_list)

        # Парсинг JSON, очистка от NaN значений в ключевом столбце
        first_key = list(schema.keys())[0]
        if "raw_data" in merged_df.columns:
            df_parsed = merged_df.withColumn("json_data", from_json(col("raw_data"), json_schema))
            # Выбор всех колонок из json_data
            df_selected = df_parsed.select("json_data.*")
            # Удаление строк, где first_key содержит null
            df_filtered = df_selected.dropna(subset=[first_key])
        else:
            df_filtered = merged_df.dropna(subset=[first_key])

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

        # Проверяем результат
        print("Результативный датафрейм:")
        df_final.show(10)

        # Сохраняем результат в CSV формате
        df_final.write.mode("overwrite").option("header", "true").csv(final_output_path)

        print(f"Данные успешно сохранены в {final_output_path}")
    
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        # Закрытие Spark-сессии
        spark.stop()
    
#-------------------------------------------------------------------------------------------------------------------------------------------------------
# Вызов функции для импорта данных о логинах клиентов
kafka_topic_logins = "e_krylova_logins_info"
folder_name_logins='logins'
schema_logins = {"client_id": DoubleType(), "login_date": StringType(), "ip_address": StringType(), 
          "location": StringType(), "device":  StringType()}
import_table_kafka_to_hdfs(hdfs_server, user_hdfs, kafka_topic_logins, kafka_bootstrap_servers, folder_name_logins, schema_logins)
