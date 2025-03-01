# Отправка сгенерированных данных в Kafka через producer
from confluent_kafka import Producer
import json
from datetime import datetime, date, timedelta
from confluent_kafka.admin import AdminClient, NewTopic, NewTopic
import pandas as pd
from faker import Faker
import numpy as np

#---------------------------------------------------------------------------------------------
# ГЕНЕРАЦИЯ ДАННЫХ
#---------------------------------------------------------------------------------------------
# Параметры генерации данных
num_clients = 1000
fake = Faker()

# Функция добавления дубликатов
def add_duplicates(df):   
    # Вычисляем количество строк для дублирования (5% от общего количества строк)
    num_rows_to_duplicate = max(1, int(len(df) * 0.05))
    # Случайным образом выбираем строки для дублирования
    duplicated_rows = df.sample(n=num_rows_to_duplicate, replace=True)
    # Добавляем дублируемые строки обратно в DataFrame
    new_df = pd.concat([df, duplicated_rows], ignore_index=True)
    # Перемешиваем строки, чтобы дубликаты не шли подряд
    new_df = new_df.sample(frac=1).reset_index(drop=True)
    return new_df
#---------------------------------------------------------------------------------------------
# Функция получения данных о клиентах
def get_client_info():
    client_data = []
    for client_id in range(1, num_clients + 1):
        client_data.append({
            'client_id': client_id,
            'client_first_name': fake.first_name(),
            'client_last_name': fake.last_name(),
            'client_email': fake.email(),
            'client_phone': fake.phone_number(),
            'client_address': fake.address(),
            'client_birthday': fake.date_of_birth()
        })
    client_df = pd.DataFrame(client_data)

    # Добавление пропусков 
    for column in ['client_id', 'client_first_name', 'client_last_name', 'client_email', 'client_phone', 'client_address', 'client_birthday']:
        mask = np.random.rand(len(client_df)) < 0.07
        client_df.loc[mask, column] = np.nan
    
    # Добавление дубликатов
    client_df = add_duplicates(client_df)

    return client_df

#---------------------------------------------------------------------------------------------
# ЗАГРУЗКА СГЕНЕРИРОВАННЫХ ДАННЫХ В KAFKA
#---------------------------------------------------------------------------------------------
# Адрес удаленного сервера Kafka
kafka_server = '172.17.0.13:9092'  # сервер Kafka
# Имена топиков Kafka
kafka_topic_clients = "e_krylova_clients_info"

#---------------------------------------------------------------------------------------------
# ОЧИСТКА ТОПИКА

# Настройка подключения к Kafka
admin_client = AdminClient({'bootstrap.servers': kafka_server})

def recreate_topic(topic_name, num_partitions=1, replication_factor=1):
    # Удаляем топик
    try:
        fs = admin_client.delete_topics([topic_name])
        for topic, f in fs.items():
            f.result()  # Ждем завершения операции
            print(f"Топик {topic} удален.")
    except Exception as e:
        print(f"Ошибка при удалении топика {topic_name}: {e}")
    
    # Создаем новый топик
    new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    try:
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            f.result()  # Ждем завершения операции
            print(f"Топик {topic} создан заново.")
    except Exception as e:
        print(f"Ошибка при создании топика {topic_name}: {e}")

# Очистка конкретных топиков перед загрузкой новых данных
recreate_topic(kafka_topic_clients)

#---------------------------------------------------------------------------------------------
# ЗАГРУЗКА ДАННЫХ В KAFKA

# Создание Kafka Producer
def create_kafka_producer(kafka_server):
    config = {
        'bootstrap.servers': kafka_server,  # Адрес сервера Kafka
        'acks': 'all',  # Подтверждение доставки (можно настроить)
        'enable.idempotence': True          # Включаем идемпотентность для предотвращения дублирования
    }
    producer = Producer(config)
    return producer

# Отправка сообщения в Kafka
def send_message(producer, topic, message):
    try:
        producer.produce(topic, value=message, callback=delivery_report)
        producer.poll(0)  # Вызываем poll для обработки событий отправки
    except BufferError:
        print("Ошибка: Очередь сообщений переполнена")
        producer.poll(1)  # Очистка очереди

# Функция обратного вызова для отчетов о доставке
def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка отправки сообщения: {err}")
    else:
        print(f"Сообщение успешно отправлено в топик {msg.topic()} с offset {msg.offset()}")

# Отправка датафрейма в Kafka
def send_df_to_kafka(df, topic, producer):
    for _, row in df.iterrows():
        # Функция преобразования значения в строку ISO
        def convert_value(value):
            if isinstance(value, datetime):
                return value.isoformat()  # Преобразование datetime в строку ISO
            elif isinstance(value, date):
                return value.strftime('%Y-%m-%d')  # Преобразование date в строку
            else:
                return value  # Оставляем остальные значения без изменений
    # Создаем список словарей из DataFrame, преобразуя значения
        row_dict = row.to_dict()
        for key, val in row_dict.items():
            row_dict[key] = convert_value(val)
        # Преобразование в JSON
        message = json.dumps(row_dict).encode('utf-8')
        send_message(producer, topic, message)
    # Ожидание завершения отправки всех сообщений
    producer.flush()

# Использование
if __name__ == "__main__":  
    # Получаем датафреймы с информациями о клиентах, активностях, логинах, платежах и транзакциях
    clients_df = get_client_info()

    # Создание продьюсеров Kafka
    producer_clients = create_kafka_producer(kafka_server)
    
    # Отправка данных из DataFrame в Kafka 
    send_df_to_kafka(clients_df, kafka_topic_clients, producer_clients)

    print("Данные успешно отправлены в Kafka")