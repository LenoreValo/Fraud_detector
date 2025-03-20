# Отправка сгенерированных данных в Kafka через producer
from confluent_kafka import Producer
import json
from datetime import datetime, date, timedelta
from confluent_kafka.admin import AdminClient, NewTopic, NewTopic
import pandas as pd
from faker import Faker
import numpy as np
import random

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
# Функция получения данных о клиентских логинах
def get_logins_info():
    max_logins_per_client = 100
    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Генерация данных для клиентских логинов
    login_data = []
    for client_id in range(1, num_clients + 1):
        num_logins = random.randint(1, max_logins_per_client)
        for _ in range(num_logins):
            login_date = random_date(start_date, end_date)
            login_data.append({
                'client_id': client_id,
                'login_date': login_date,
                'ip_address': fake.ipv4(),
                'location': f"{random.uniform(-90, 90)}, {random.uniform(-180, 180)}",
                'device': fake.user_agent()
            })
    login_df = pd.DataFrame(login_data)

    # Добавление пропусков
    for column in ['client_id', 'login_date', 'ip_address', 'location', 'device']:
        mask = np.random.rand(len(login_df)) < 0.05
        login_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(login_df) > 0:
        mask = np.random.rand(len(login_df)) < 0.05
        login_df.loc[mask, 'login_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12), day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]
    # Добавляем дубликаты
    login_df = add_duplicates(login_df)

    return login_df
#---------------------------------------------------------------------------------------------
# ЗАГРУЗКА СГЕНЕРИРОВАННЫХ ДАННЫХ В KAFKA
#---------------------------------------------------------------------------------------------
# Адрес удаленного сервера Kafka
kafka_server = '172.17.0.13:9092'  # сервер Kafka
# Имя топика Kafka
kafka_topic_logins = "e_krylova_logins_info"
#---------------------------------------------------------------------------------------------
# ОЧИСТКА ТОПИКА

# Настройка подключения к Kafka
admin_client = AdminClient({'bootstrap.servers': kafka_server})
# Функция пересоздания топика
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
recreate_topic(kafka_topic_logins)
#---------------------------------------------------------------------------------------------
# ЗАГРУЗКА ДАННЫХ В KAFKA

# Создание Kafka Producer
def create_kafka_producer(kafka_server):
    config = {
        'bootstrap.servers': kafka_server,  # Адрес сервера Kafka
        'acks': 'all',
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

if __name__ == "__main__":  
    # Получаем датафреймы с информациями о логинах
    logins_df = get_logins_info()
        
    # Создание продьюсеров Kafka
    producer_logins = create_kafka_producer(kafka_server)
        
    # Отправка данных из DataFrame в Kafka 
    send_df_to_kafka(logins_df, kafka_topic_logins, producer_logins)
    
    print("Данные успешно отправлены в Kafka")