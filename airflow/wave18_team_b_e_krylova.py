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
    'retries': 2,
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

# Определение последовательности выполнения
hello_task >> [client_generator_to_kafka, payments_generator_to_kafka]
client_generator_to_kafka >> client_activity_generator_to_kafka
payments_generator_to_kafka >> logins_generator_to_kafka
[client_activity_generator_to_kafka, logins_generator_to_kafka] >> transactions_generator_to_kafka 


#hello_task >> [client_generator_to_kafka, 
#               client_activity_generator_to_kafka, 
#               logins_generator_to_kafka,
#               payments_generator_to_kafka,  
#               transactions_generator_to_kafka]