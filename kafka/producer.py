# Отправка сгенерированных данных в Kafka через producer
# запуск через bash: python -m kafka.producer
from confluent_kafka import Producer
import json
from datetime import datetime, date
from generators.clients_generator import get_client_info
from generators.client_activity_generator import get_client_activity_info
from generators.logins_generator import get_logins_info
from generators.payments_generator import get_payments_info
from generators.transactions_generator import get_transactions_info
from confluent_kafka.admin import AdminClient, NewTopic, NewTopic

#---------------------------------------------------------------------------------------------
# Глобальные переменные

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