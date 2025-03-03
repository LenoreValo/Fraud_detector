from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

dag = DAG(
    dag_id = 'wave18_team_b_e_krylova',
    default_args=default_args,
    description='Simple pipeline DAG from Lenore',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1
)

# Приветственная таска
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)
#---------------------------------------------------------------------------------------------------------------------
# Задача по отправке сгенерированных данных о клиентах в Kafka
client_generator_to_kafka = SSHOperator(
    task_id='client_generator_to_kafka',
    ssh_conn_id='e_krylova_ssh',
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
    command="""
        source /home/e.krylova/study_project/my_env/bin/activate && 
        python3 /home/e.krylova/study_project/generators_kafka/transactions_generator_to_kafka.py
    """,
    dag=dag,
)
#---------------------------------------------------------------------------------------------------------------------
# Задача по передачи данных о клиентах из Kafka в HDFS
kafka_clients_to_hdfs = SSHOperator(
    task_id='kafka_clients_to_hdfs',
    ssh_conn_id='e_krylova_ssh',
    command="spark-submit --conf spark.ui.port=5050 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_clients_to_hdfs.py",
    dag=dag,
)
# Задача по передачи данных об активностях клиентов из Kafka в HDFS
kafka_clients_activity_to_hdfs = SSHOperator(
    task_id='kafka_clients_activity_to_hdfs',
    ssh_conn_id='e_krylova_ssh',
    command="spark-submit --conf spark.ui.port=5050 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_clients_activity_to_hdfs.py",
    dag=dag,
)
# Задача по передачи данных о логинах из Kafka в HDFS
kafka_logins_to_hdfs = SSHOperator(
    task_id='kafka_logins_to_hdfs',
    ssh_conn_id='e_krylova_ssh',
    command="spark-submit --conf spark.ui.port=5050 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_logins_to_hdfs.py",
    dag=dag,
)
# Задача по передачи данных о платежах из Kafka в HDFS
kafka_payments_to_hdfs = SSHOperator(
    task_id='kafka_payments_to_hdfs',
    ssh_conn_id='e_krylova_ssh',
    command="spark-submit --conf spark.ui.port=5050 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_payments_to_hdfs.py",
    dag=dag,
)
# Задача по передачи данных о транзакциях из Kafka в HDFS
kafka_transactions_to_hdfs = SSHOperator(
    task_id='kafka_transactions_to_hdfs',
    ssh_conn_id='e_krylova_ssh',
    command="spark-submit --conf spark.ui.port=5050 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 /home/e.krylova/study_project/spark/kafka_transactions_to_hdfs.py",
    dag=dag,
)
#---------------------------------------------------------------------------------------------------------------------
# Импорт данных с информацией о клиентах из HDFS в GreenPlum слой RAW
hdfs_to_raw_greenplum_clients = PostgresOperator(
        task_id='hdfs_to_raw_greenplum_clients',
        postgres_conn_id='gp_krylova_wave18',
        sql='CALL raw.create_hdfs_to_raw_clients_e_krylova();'
)

# Импорт данных с информацией о клиентах из HDFS в GreenPlum слой RAW
hdfs_to_raw_greenplum_clients_activity = PostgresOperator(
        task_id='hdfs_to_raw_greenplum_clients_activity',
        postgres_conn_id='gp_krylova_wave18',
        sql='CALL raw.create_hdfs_to_raw_clients_activity_e_krylova();'
)

# Импорт данных с информацией о клиентах из HDFS в GreenPlum слой RAW
hdfs_to_raw_greenplum_logins = PostgresOperator(
        task_id='hdfs_to_raw_greenplum_logins',
        postgres_conn_id='gp_krylova_wave18',
        sql='CALL raw.create_hdfs_to_raw_logins_e_krylova();'
)

# Импорт данных с информацией о клиентах из HDFS в GreenPlum слой RAW
hdfs_to_raw_greenplum_payments = PostgresOperator(
        task_id='hdfs_to_raw_greenplum_payments',
        postgres_conn_id='gp_krylova_wave18',
        sql='CALL raw.create_hdfs_to_raw_payments_e_krylova();'
)
# Импорт данных с информацией о клиентах из HDFS в GreenPlum слой RAW
hdfs_to_raw_greenplum_transactions = PostgresOperator(
        task_id='hdfs_to_raw_greenplum_transactions',
        postgres_conn_id='gp_krylova_wave18',
        sql='CALL raw.create_hdfs_to_raw_transactions_e_krylova();'
)
#---------------------------------------------------------------------------------------------------------------------


# Определение последовательности выполнения
hello_task >> [client_generator_to_kafka, payments_generator_to_kafka]
client_generator_to_kafka >> client_activity_generator_to_kafka
payments_generator_to_kafka >> logins_generator_to_kafka
[client_activity_generator_to_kafka, logins_generator_to_kafka] >> transactions_generator_to_kafka 

transactions_generator_to_kafka  >> kafka_clients_to_hdfs >> kafka_clients_activity_to_hdfs >> kafka_logins_to_hdfs >> kafka_payments_to_hdfs >> kafka_transactions_to_hdfs

kafka_transactions_to_hdfs >> [hdfs_to_raw_greenplum_clients, 
                               hdfs_to_raw_greenplum_clients_activity, 
                               hdfs_to_raw_greenplum_logins, 
                               hdfs_to_raw_greenplum_payments, 
                               hdfs_to_raw_greenplum_transactions]




