from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.task_group import TaskGroup

def print_hello():
    print(f'Hello from Lenore! I present you my simple pipeline.')

default_args = {
    'owner': 'e_krylova',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = 'wave18_team_b_e_krylova',
    default_args=default_args,
    description='Simple pipeline DAG from Lenore',
    schedule_interval='50 5 * * 1-5',  # Каждый будний день в 8:50 по МСК
    catchup=False,
    max_active_runs=1
) as dag:

    # Приветственная таска
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
        dag=dag,
    )
#---------------------------------------------------------------------------------------------------------------------
    with TaskGroup("generators_to_kafka") as generators_to_kafka:
        # Задача по отправке сгенерированных данных о клиентах в Kafka
        client_generator_to_kafka = SSHOperator(
            task_id='client_generator_to_kafka',
            ssh_conn_id='e_krylova_ssh',
            retries=3,
            retry_delay=timedelta(minutes=1),
            command="""
                source /home/e.krylova/study_project/my_env/bin/activate && 
                python3 /home/e.krylova/study_project/generators_kafka/client_generator_to_kafka.py
            """,
            dag=dag,
        )
        # Задача по отправке сгенерированных данных об активностях клиентов в Kafka
        client_activity_generator_to_kafka = SSHOperator(
            task_id='client_activity_generator_to_kafka',
            ssh_conn_id='e_krylova_ssh',
            retries=3,
            retry_delay=timedelta(minutes=1),
            command="""
                source /home/e.krylova/study_project/my_env/bin/activate && 
                python3 /home/e.krylova/study_project/generators_kafka/client_activity_generator_to_kafka.py
            """,
            dag=dag,
        )
        # Задача по отправке сгенерированных данных о логинах клиентов в Kafka
        logins_generator_to_kafka = SSHOperator(
            task_id='logins_generator_to_kafka',
            ssh_conn_id='e_krylova_ssh',
            retries=3,
            retry_delay=timedelta(minutes=1),
            command="""
                source /home/e.krylova/study_project/my_env/bin/activate && 
                python3 /home/e.krylova/study_project/generators_kafka/logins_generator_to_kafka.py
            """,
            dag=dag,
        )
        # Задача по отправке сгенерированных данных о платежах клиентов в Kafka
        payments_generator_to_kafka = SSHOperator(
            task_id='payments_generator_to_kafka',
            ssh_conn_id='e_krylova_ssh',
            retries=3,
            retry_delay=timedelta(minutes=1),
            command="""
                source /home/e.krylova/study_project/my_env/bin/activate && 
                python3 /home/e.krylova/study_project/generators_kafka/payments_generator_to_kafka.py
            """,
            dag=dag,
        )
        # Задача по отправке сгенерированных данных о транзакциях клиентов в Kafka
        transactions_generator_to_kafka = SSHOperator(
            task_id='transactions_generator_to_kafka',
            ssh_conn_id='e_krylova_ssh',
            retries=3,
            retry_delay=timedelta(minutes=1),
            command="""
                source /home/e.krylova/study_project/my_env/bin/activate && 
                python3 /home/e.krylova/study_project/generators_kafka/transactions_generator_to_kafka.py
            """,
            dag=dag,
        )
        [client_generator_to_kafka, payments_generator_to_kafka] >> transactions_generator_to_kafka 
        transactions_generator_to_kafka  >> [logins_generator_to_kafka, client_activity_generator_to_kafka]

        #client_generator_to_kafka >> client_activity_generator_to_kafka >> logins_generator_to_kafka >> payments_generator_to_kafka >> transactions_generator_to_kafka
#---------------------------------------------------------------------------------------------------------------------
    with TaskGroup("kafka_to_hdfs") as kafka_to_hdfs:
        # Задача по передачи данных о клиентах из Kafka в HDFS
        kafka_clients_to_hdfs = SSHOperator(
            task_id='kafka_clients_to_hdfs',
            ssh_conn_id='e_krylova_ssh',
            command="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_clients_to_hdfs.py",
            dag=dag,
        )
        # Задача по передачи данных об активностях клиентов из Kafka в HDFS
        kafka_clients_activity_to_hdfs = SSHOperator(
            task_id='kafka_clients_activity_to_hdfs',
            ssh_conn_id='e_krylova_ssh',
            command="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_clients_activity_to_hdfs.py",
            dag=dag,
        )
        # Задача по передачи данных о логинах из Kafka в HDFS
        kafka_logins_to_hdfs = SSHOperator(
            task_id='kafka_logins_to_hdfs',
            ssh_conn_id='e_krylova_ssh',
            command="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_logins_to_hdfs.py",
            dag=dag,
        )
        # Задача по передачи данных о платежах из Kafka в HDFS
        kafka_payments_to_hdfs = SSHOperator(
            task_id='kafka_payments_to_hdfs',
            ssh_conn_id='e_krylova_ssh',
            command="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_payments_to_hdfs.py",
            dag=dag,
        )
        # Задача по передачи данных о транзакциях из Kafka в HDFS
        kafka_transactions_to_hdfs = SSHOperator(
            task_id='kafka_transactions_to_hdfs',
            ssh_conn_id='e_krylova_ssh',
            command="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_transactions_to_hdfs.py",
            dag=dag,
        )

        # --conf spark.ui.port=5050 - явное указание порта в spark-submit

        kafka_clients_to_hdfs >> kafka_clients_activity_to_hdfs >> kafka_logins_to_hdfs >> kafka_payments_to_hdfs >> kafka_transactions_to_hdfs
#---------------------------------------------------------------------------------------------------------------------
    with TaskGroup("hdfs_to_raw_greenplum") as hdfs_to_raw_greenplum:
    # Импорт данных с информацией о клиентах из HDFS в GreenPlum слой RAW
        hdfs_to_raw_greenplum_clients = PostgresOperator(
                task_id='hdfs_to_raw_greenplum_clients',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL raw.create_hdfs_to_raw_clients_e_krylova();'
        )

        # Импорт данных с информацией об активностях из HDFS в GreenPlum слой RAW
        hdfs_to_raw_greenplum_clients_activity = PostgresOperator(
                task_id='hdfs_to_raw_greenplum_clients_activity',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL raw.create_hdfs_to_raw_clients_activity_e_krylova();'
        )

        # Импорт данных с информацией о логинах из HDFS в GreenPlum слой RAW
        hdfs_to_raw_greenplum_logins = PostgresOperator(
                task_id='hdfs_to_raw_greenplum_logins',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL raw.create_hdfs_to_raw_logins_e_krylova();'
        )

        # Импорт данных с информацией о платежах из HDFS в GreenPlum слой RAW
        hdfs_to_raw_greenplum_payments = PostgresOperator(
                task_id='hdfs_to_raw_greenplum_payments',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL raw.create_hdfs_to_raw_payments_e_krylova();'
        )
        # Импорт данных с информацией о транзакциях из HDFS в GreenPlum слой RAW
        hdfs_to_raw_greenplum_transactions = PostgresOperator(
                task_id='hdfs_to_raw_greenplum_transactions',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL raw.create_hdfs_to_raw_transactions_e_krylova();'
        )
#---------------------------------------------------------------------------------------------------------------------
    with TaskGroup("raw_to_ods") as raw_to_ods:
            # Загрузка данных с информацией о клиентах из RAW в ODS
            raw_to_ods_clients = PostgresOperator(
                    task_id='raw_to_ods_clients',
                    postgres_conn_id='gp_krylova_wave18',
                    sql='CALL ods.transform_load_clients_e_krylova();'
            )
            # Загрузка данных с информацией об активностях из RAW в ODS
            raw_to_ods_clients_activity = PostgresOperator(
                    task_id='raw_to_ods_clients_activity',
                    postgres_conn_id='gp_krylova_wave18',
                    sql='CALL ods.transform_load_clients_activity_e_krylova();'
            )
            # Загрузка данных с информацией о логинах из RAW в ODS
            raw_to_ods_logins = PostgresOperator(
                    task_id='raw_to_ods_logins',
                    postgres_conn_id='gp_krylova_wave18',
                    sql='CALL ods.transform_load_logins_e_krylova();'
            )
            # Загрузка данных с информацией о платежах из RAW в ODS
            raw_to_ods_payments = PostgresOperator(
                    task_id='raw_to_ods_payments',
                    postgres_conn_id='gp_krylova_wave18',
                    sql='CALL ods.transform_load_payments_e_krylova();'
            )
            # Загрузка данных с информацией о транзакциях из RAW в ODS
            raw_to_ods_transactions = PostgresOperator(
                    task_id='raw_to_ods_transactions',
                    postgres_conn_id='gp_krylova_wave18',
                    sql='CALL ods.transform_load_transactions_e_krylova();'
            )
#---------------------------------------------------------------------------------------------------------------------
    # Загрузка данных с данными из ODS в DDS
    ods_to_dds = PostgresOperator(
            task_id='ods_to_dds',
            postgres_conn_id='gp_krylova_wave18',
            sql='CALL dds.create_and_load_data_e_krylova();'
    )
#---------------------------------------------------------------------------------------------------------------------
     # Загрузка данных с данными из DDS в DM
    with TaskGroup("dds_to_dm") as dds_to_dm:
        dds_to_dm_clients_activity = PostgresOperator(
                task_id='dds_to_dm_clients_activity',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL dm.load_clients_activity_e_krylova();'
        )
        dds_to_dm_payments = PostgresOperator(
                task_id='dds_to_dm_payments',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL dm.load_payments_e_krylova();'
        )
        dds_to_dm_transactions = PostgresOperator(
                task_id='dds_to_dm_transactions',
                postgres_conn_id='gp_krylova_wave18',
                sql='CALL dm.load_transactions_e_krylova();'
        )
#---------------------------------------------------------------------------------------------------------------------
    with TaskGroup("dm_to_clickhouse") as dm_to_clickhouse:  
        # 1. Удаление внешней таблицы с информацией о клиентах и их активностях и логинах (если существует)
        drop_client_ext = ClickHouseOperator(
            task_id="drop_external_table_clients",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                DROP TABLE IF EXISTS wave18_team_b.gp_clients_activity_e_krylova;
            """,
        )
        # 2. Создание внешней таблицы с информацией о клиентах и их активностях и логинах в ClickHouse
        create_client_ext = ClickHouseOperator(
            task_id="create_external_table_clients",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
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
            """,
        )
        # 3. Удаление локальной таблицы с информацией о клиентах и их активностях и логинах в ClickHouse (если существует)
        drop_client_local = ClickHouseOperator(
            task_id="drop_local_table_clients",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                DROP TABLE IF EXISTS wave18_team_b.clients_activity_e_krylova;
            """,
        )
        # 4. Создание локальной таблицы в ClickHouse
        create_client_local = ClickHouseOperator(
            task_id="create_local_table_clients",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
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
            """,
        )
        # 5. Копирование данных из внешней таблицы в локальную таблицу
        insert_client_local = ClickHouseOperator(
            task_id="insert_local_table_clients",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
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
            """,
        )
#--------------------------------------------------------
        # 1. Удаление внешней таблицы с информацией о транзакциях (если существует)
        drop_transactions_ext = ClickHouseOperator(
            task_id="drop_external_table_transactions",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                drop table if exists gp_transactions_e_krylova;
            """,
        )
        # 2. Создание внешней таблицы с информацией о транзакциях в ClickHouse
        create_transactions_ext = ClickHouseOperator(
            task_id="create_external_table_transactions",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                create table gp_transactions_e_krylova(
                    client_id UInt32,
                    client_first_name String,
                    client_last_name String,
                    client_email String,
                    client_phone String,
                    client_address String,
                    client_birthday DATE,
                    account_number String,
                    transaction_id UInt32,
                    transaction_date DateTime64(3, 'UTC'),
                    transaction_type_id UInt32,
                    transaction_type_name String,
                    currency_id UInt32,
                    currency_name String,
                    total_amount Decimal(15, 2)
                ) ENGINE = PostgreSQL('172.17.1.32:5432', 'wave18_team_b', 'transactions_e_krylova', 'gpadmin', 'gpadmin', 'dm')
                ;
            """,
        )
        # 3. Удаление локальной таблицы с информацией о транзакциях в ClickHouse (если существует)
        drop_transactions_local = ClickHouseOperator(
            task_id="drop_local_table_transactions",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                drop table if exists transactions_e_krylova;
            """,
        )
        # 4. Создание локальной таблицы о транзакциях в ClickHouse
        create_transactions_local = ClickHouseOperator(
            task_id="create_local_table_transactions",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                CREATE TABLE transactions_e_krylova
                    (
                        client_id UInt32,
                        client_first_name String,
                        client_last_name String,
                        client_email String,
                        client_phone String,
                        client_address String,
                        client_birthday DATE,
                        account_number String,
                        transaction_id UInt32,
                        transaction_date DateTime64(3, 'UTC'),
                        transaction_type_id UInt32,
                        transaction_type_name String,
                        currency_id UInt32,
                        currency_name String,
                        total_amount Decimal(15, 2)
                    ) ENGINE = MergeTree()
                    ORDER BY transaction_date;
            """,
        )
        # 5. Копирование данных из внешней таблицы в локальную таблицу о транзакциях
        insert_transactions_local = ClickHouseOperator(
            task_id="insert_local_table_transactions",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                INSERT INTO transactions_e_krylova
                SELECT * FROM gp_transactions_e_krylova;
            """,
        )
#--------------------------------------------------------
        # 1. Удаление внешней таблицы с информацией о платежах (если существует)
        drop_payments_ext = ClickHouseOperator(
            task_id="drop_external_table_payments",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                drop table if exists gp_payments_e_krylova;
            """,
        )
        # 2. Создание внешней таблицы с информацией о платежах в ClickHouse
        create_payments_ext = ClickHouseOperator(
            task_id="create_external_table_payments",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                create table gp_payments_e_krylova(
                    client_id UInt32,
                    client_first_name String,
                    client_last_name String,
                    client_email String,
                    client_phone String,
                    client_address String,
                    client_birthday DATE,
                    payment_id UInt32,
                    account_number String,
                    payment_date DateTime64(3, 'UTC'),
                    currency_id UInt32,
                    currency_name String,
                    total_amount Decimal(15, 2),
                    payment_method_id UInt32,
                    payment_method_name String
                ) ENGINE = PostgreSQL('172.17.1.32:5432', 'wave18_team_b', 'payments_e_krylova', 'gpadmin', 'gpadmin', 'dm')
                ;
            """,
        )
        # 3. Удаление локальной таблицы с информацией о платежах в ClickHouse (если существует)
        drop_payments_local = ClickHouseOperator(
            task_id="drop_local_table_payments",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                drop table if exists payments_e_krylova;
            """,
        )
        # 4. Создание локальной таблицы о платежах в ClickHouse
        create_payments_local = ClickHouseOperator(
            task_id="create_local_table_payments",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                CREATE TABLE payments_e_krylova
                    (
                        client_id UInt32,
                        client_first_name String,
                        client_last_name String,
                        client_email String,
                        client_phone String,
                        client_address String,
                        client_birthday DATE,
                        payment_id UInt32,
                        account_number String,
                        payment_date DateTime64(3, 'UTC'),
                        currency_id UInt32,
                        currency_name String,
                        total_amount Decimal(15, 2),
                        payment_method_id UInt32,
                        payment_method_name String
                    ) ENGINE = MergeTree()
                    ORDER BY payment_date;
            """,
        )
        # 5. Копирование данных из внешней таблицы в локальную таблицу о платежах
        insert_payments_local = ClickHouseOperator(
            task_id="insert_local_table_payments",
            clickhouse_conn_id="e_krylova_clickhouse",
            database='default',
            sql="""
                INSERT INTO payments_e_krylova
                SELECT * FROM gp_payments_e_krylova;
            """,
        )


        drop_client_ext >> create_client_ext >> drop_client_local >> create_client_local >> insert_client_local
        drop_transactions_ext >> create_transactions_ext >> drop_transactions_local >> create_transactions_local >> insert_transactions_local
        drop_payments_ext >> create_payments_ext >> drop_payments_local >> create_payments_local >> insert_payments_local

# Определение последовательности выполнения
hello_task >> generators_to_kafka
generators_to_kafka >> kafka_to_hdfs 
kafka_to_hdfs >> hdfs_to_raw_greenplum
hdfs_to_raw_greenplum >> raw_to_ods
raw_to_ods >> ods_to_dds >> dds_to_dm >> dm_to_clickhouse



