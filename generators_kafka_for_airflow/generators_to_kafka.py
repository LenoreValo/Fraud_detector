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
def get_client_activity_info():
    max_activities_per_client = 100
    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Генерация данных для клиентской активности
    activity_data = []
    for client_id in range(1, num_clients + 1):
        num_activities = random.randint(1, max_activities_per_client)
        for _ in range(num_activities):
            activity_date = random_date(start_date, end_date)
            activity_data.append({
                'client_id': client_id,
                'activity_date': activity_date,
                'activity_type': np.random.choice(['view_account', 'transfer_funds', 'pay_bill', 'login', 'logout']),
                'activity_location': fake.uri_path(),
                'ip_address': fake.ipv4(),
                'device': fake.user_agent()
            })
    activity_df = pd.DataFrame(activity_data)

    # Добавление пропусков
    for column in ['client_id', 'activity_date', 'activity_type', 'activity_location', 'ip_address', 'device']:
        mask = np.random.rand(len(activity_df)) < 0.05
        activity_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(activity_df) > 0:
        mask = np.random.rand(len(activity_df)) < 0.05
        activity_df.loc[mask, 'activity_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12), day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]
    # Добавляем дубликаты
    activity_df = add_duplicates(activity_df)

    return activity_df

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
# Функция получения данных о платежах клиентов
def get_payments_info():
    max_payments_per_client = 50
    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Генерация данных для платежей
    payment_data = []
    for client_id in range(1, num_clients + 1):
        num_payments = random.randint(1, max_payments_per_client)
        for _ in range(num_payments):
            payment_date = random_date(start_date, end_date)
            currency = np.random.choice(['USD', 'RUB'])
            amount = round(np.random.uniform(100, 10000), 2) if currency == 'USD' else round(np.random.uniform(9000, 900000), 2)
            payment_data.append({
                'client_id': client_id,
                'payment_id': np.random.randint(1000, 10000),
                'payment_date': payment_date,
                'currency': currency,
                'amount': amount,
                'payment_method': np.random.choice(['credit_card', 'debit_card', 'bank_transfer', 'e_wallet'])
            })
    payment_df = pd.DataFrame(payment_data)

    # Добавление пропусков
    for column in ['client_id', 'payment_id', 'payment_date', 'currency', 'amount', 'payment_method']:
        mask = np.random.rand(len(payment_df)) < 0.05
        payment_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(payment_df) > 0:
        mask = np.random.rand(len(payment_df)) < 0.05
        payment_df.loc[mask, 'payment_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12), day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]
    # Добавляем дубликаты
    payment_df = add_duplicates(payment_df)

    return payment_df
#---------------------------------------------------------------------------------------------
# Функция получения данных о транзакциях
def get_transactions_info():
    max_transactions_per_client = 150
    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Генерация данных для банковских транзакций
    transaction_data = []
    for client_id in range(1, num_clients + 1):
        num_transactions = random.randint(1, max_transactions_per_client)
        for _ in range(num_transactions):
            transaction_date = random_date(start_date, end_date)
            currency = np.random.choice(['USD', 'RUB'])
            amount = round(np.random.uniform(100, 10000), 2) if currency == 'USD' else round(np.random.uniform(9000, 900000), 2)
            
            # Добавление аномально больших сумм
            if random.random() < 0.01:
                amount *= 10

            transaction_data.append({
                'client_id': client_id,
                'transaction_id': np.random.randint(1000, 10000),
                'transaction_date': transaction_date,
                'transaction_type': np.random.choice(['deposit', 'withdrawal', 'transfer']),
                'account_number': fake.iban(),
                'currency': currency,
                'amount': amount
            })

    # Добавление аномально большого кол-ва транзакций в один день для 4 клиентов
    anomalous_clients = random.sample(range(1, num_clients + 1), 4)
    for client_id in anomalous_clients:
        anomalous_dates = [random_date(start_date, end_date) for _ in range(2)]
        for date in anomalous_dates:
            for _ in range(10):  
                transaction_data.append({
                    'client_id': client_id,
                    'transaction_id': np.random.randint(1000, 10000),
                    'transaction_date': date,
                    'transaction_type': np.random.choice(['deposit', 'withdrawal', 'transfer']),
                    'account_number': fake.iban(),
                    'currency': np.random.choice(['USD', 'RUB']),
                    'amount': round(np.random.uniform(100, 10000), 2),
                    'record_saved_at': datetime.now()
                })
    # Создаем датафрейм
    transaction_df = pd.DataFrame(transaction_data)

    # Добавление пропусков
    for column in ['client_id', 'transaction_id', 'transaction_date', 'transaction_type', 'account_number', 'currency','amount']:
        mask = np.random.rand(len(transaction_df)) < 0.04
        transaction_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(transaction_df) > 0:
        mask = np.random.rand(len(transaction_df)) < 0.05
        transaction_df.loc[mask, 'transaction_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12), day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]
    # Добавляем дубликаты
    transaction_df = add_duplicates(transaction_df)
    
    return transaction_df
#---------------------------------------------------------------------------------------------
# ЗАГРУЗКА СГЕНЕРИРОВАННЫХ ДАННЫХ В KAFKA
#---------------------------------------------------------------------------------------------
# Адрес удаленного сервера Kafka
kafka_server = '172.17.0.13:9092'  # сервер Kafka
# Имена топиков Kafka
kafka_topic_clients = "e_krylova_clients_info"
kafka_topic_activity = "e_krylova_client_activity_info"
kafka_topic_logins = "e_krylova_logins_info"
kafka_topic_payments = "e_krylova_payments_info"
kafka_topic_transactions = "e_krylova_transactions_info"
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
recreate_topic(kafka_topic_clients)
recreate_topic(kafka_topic_activity)
recreate_topic(kafka_topic_logins)
recreate_topic(kafka_topic_payments)
recreate_topic(kafka_topic_transactions)

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
    # Получаем датафреймы с информациями о клиентах, активностях, логинах, платежах и транзакциях
    clients_df = get_client_info()
    client_activity_df = get_client_activity_info()
    logins_df = get_logins_info()
    payments_df = get_payments_info()
    transactions_df = get_transactions_info()
    
    # Создание продьюсеров Kafka
    producer_clients = create_kafka_producer(kafka_server)
    producer_client_activity = create_kafka_producer(kafka_server)
    producer_logins = create_kafka_producer(kafka_server)
    producer_payments = create_kafka_producer(kafka_server)
    producer_transaction = create_kafka_producer(kafka_server)
    
    # Отправка данных из DataFrame в Kafka 
    send_df_to_kafka(clients_df, kafka_topic_clients, producer_clients)
    send_df_to_kafka(client_activity_df, kafka_topic_activity, producer_client_activity)
    send_df_to_kafka(logins_df, kafka_topic_logins, producer_logins)
    send_df_to_kafka(payments_df, kafka_topic_payments, producer_payments)
    send_df_to_kafka(transactions_df, kafka_topic_transactions, producer_transaction)

    print("Данные успешно отправлены в Kafka")