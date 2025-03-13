import psycopg2
from clickhouse_driver import Client

# Настройки подключения к Greenplum
GREENPLUM_CONFIG = {
    "host": "172.17.1.32",
    "port": 5432,
    "database": "wave18_team_b",
    "user": "gpadmin",
    "password": "gpadmin",
}

# Настройки подключения к ClickHouse
CLICKHOUSE_CONFIG = {
    "host": "172.17.1.18",  # Замените на хост ClickHouse
    "port": 8123,
    "user": "",
    "password": "",
    "database": "default",
}

# Создаем клиента для ClickHouse
clickhouse_client = Client(**CLICKHOUSE_CONFIG)

# Подключаемся к Greenplum
def connect_to_greenplum():
    return psycopg2.connect(**GREENPLUM_CONFIG)

# Шаг 1: Удаление внешней таблицы в ClickHouse (если существует)
def drop_external_table():
    query = """
        DROP TABLE IF EXISTS wave18_team_b.gp_clients_activity_e_krylova;
    """
    clickhouse_client.execute(query)
    print("Внешняя таблица удалена (если существовала).")

# Шаг 2: Создание внешней таблицы в ClickHouse
def create_external_table():
    query = """
        CREATE TABLE wave18_team_b.gp_clients_activity_e_krylova (
            client_id UInt32,
            client_first_name String,
            client_last_name String,
            client_email String,
            client_phone String,
            client_address String,
            client_birthday DATE,
            activity_date DateTime64(3, 'UTC'),
            activity_type_id Int,
            activity_type_name String,
            activity_location String, 
            ip_address_activity String,
            activity_device String, 
            login_date DateTime64(3, 'UTC'),
            ip_address_login String,
            login_location String,
            login_device String
        ) ENGINE = PostgreSQL('172.17.1.32:5432', 'wave18_team_b', 'clients_activity_logins_e_krylova', 'gpadmin', 'gpadmin', 'dm');
    """
    clickhouse_client.execute(query)
    print("Внешняя таблица создана.")

# Шаг 3: Удаление локальной таблицы в ClickHouse (если существует)
def drop_local_table():
    query = """
        DROP TABLE IF EXISTS wave18_team_b.clients_activity_e_krylova;
    """
    clickhouse_client.execute(query)
    print("Локальная таблица удалена (если существовала).")

# Шаг 4: Создание локальной таблицы в ClickHouse
def create_local_table():
    query = """
        CREATE TABLE wave18_team_b.clients_activity_e_krylova (
            client_id UInt32,
            client_first_name String,
            client_last_name String,
            client_email String,
            client_phone String,
            client_address String,
            client_birthday DATE,
            activity_date DateTime64(3, 'UTC'),
            activity_type_id Int,
            activity_type_name String,
            activity_location String, 
            ip_address_activity IPv4,
            activity_device String, 
            login_date DateTime64(3, 'UTC'),
            ip_address_login IPv4,
            login_location String,
            login_device String
        ) ENGINE = MergeTree()
        ORDER BY client_id;
    """
    clickhouse_client.execute(query)
    print("Локальная таблица создана.")

# Шаг 5: Копирование данных из внешней таблицы в локальную таблицу
def copy_data():
    query = """
        INSERT INTO wave18_team_b.clients_activity_e_krylova
        SELECT 
            client_id,
            client_first_name,
            client_last_name,
            client_email,
            client_phone,
            client_address,
            client_birthday,
            activity_date,
            activity_type_id,
            activity_type_name,
            activity_location, 
            CASE
                WHEN match(ip_address_activity, '^(\d{1,3}\.){3}\d{1,3}$') AND 
                     toUInt32OrNull(splitByString('.', ip_address_activity)[1]) <= 255 AND
                     toUInt32OrNull(splitByString('.', ip_address_activity)[2]) <= 255 AND
                     toUInt32OrNull(splitByString('.', ip_address_activity)[3]) <= 255 AND
                     toUInt32OrNull(splitByString('.', ip_address_activity)[4]) <= 255
                THEN toIPv4(ip_address_activity)
                ELSE NULL
            END AS ip_address_activity,
            activity_device, 
            login_date,
            CASE
                WHEN match(ip_address_login, '^(\d{1,3}\.){3}\d{1,3}$') AND 
                     toUInt32OrNull(splitByString('.', ip_address_login)[1]) <= 255 AND
                     toUInt32OrNull(splitByString('.', ip_address_login)[2]) <= 255 AND
                     toUInt32OrNull(splitByString('.', ip_address_login)[3]) <= 255 AND
                     toUInt32OrNull(splitByString('.', ip_address_login)[4]) <= 255
                THEN toIPv4(ip_address_login)
                ELSE NULL
            END AS ip_address_login,
            login_location,
            login_device
        FROM wave18_team_b.gp_clients_activity_e_krylova;
    """
    clickhouse_client.execute(query)
    print("Данные скопированы в локальную таблицу.")

# Основная функция
def main():
    try:
        # Шаг 1: Удаление внешней таблицы
        drop_external_table()

        # Шаг 2: Создание внешней таблицы
        create_external_table()

        # Шаг 3: Удаление локальной таблицы
        drop_local_table()

        # Шаг 4: Создание локальной таблицы
        create_local_table()

        # Шаг 5: Копирование данных
        copy_data()

        print("Импорт данных завершен успешно.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()