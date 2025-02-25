-- Формирование RAW слоя с внешними данными
-- Начало транзакции
begin transaction;

create schema if not exists raw;

---------------------------------------------------------------------------------------------------------------
-- 1. Импорт данных с информацией о клиентах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists raw.hdfs_clients_e_krylova;
CREATE EXTERNAL TABLE raw.hdfs_clients_e_krylova (
	client_id text,
	client_first_name text,
	client_last_name text,
	client_email text,
	client_phone text,
	client_address text,
	client_birthday text
)
LOCATION ('pxf://user/e.krylova/study_project_b/clients/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

-- Шаг 2: Проверка данных
select * from raw.hdfs_clients_e_krylova order by client_id limit 10 ;

-----------------------------------------------------------------------------------------------------------------
-- 2. Импорт данных с информацией об активности клиентов
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists raw.hdfs_clients_activity_e_krylova;
CREATE EXTERNAL TABLE raw.hdfs_clients_activity_e_krylova (
	client_id text,
	activity_date text,
	activity_type text,
	activity_location text,
	ip_address text,
	device text
)
LOCATION ('pxf://user/e.krylova/study_project_b/clients_activity/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

-- Шаг 2: Проверка данных
select * from raw.hdfs_clients_activity_e_krylova limit 10 ;
--select client_id, activity_type from hdfs_clients_activity_e_krylova where activity_type like 'activity_type';
-- SELECT COUNT(*) AS total_rows FROM raw.hdfs_clients_activity_e_krylova;

-----------------------------------------------------------------------------------------------------------------
-- 3. Импорт данных с информацией о логинах
----------------------------------------------------------------------------------------------------------------

-- Шаг 1: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists raw.hdfs_logins_e_krylova;
CREATE EXTERNAL TABLE raw.hdfs_logins_e_krylova (
	client_id text,
	login_date text,
	ip_address text,
	location text,
	device text
)
LOCATION ('pxf://user/e.krylova/study_project_b/logins/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

-- Шаг 2: Проверка данных
select * from raw.hdfs_logins_e_krylova limit 10 ;

-----------------------------------------------------------------------------------------------------------------
-- 4. Импорт данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------

-- Шаг 1: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists raw.hdfs_payments_e_krylova;
CREATE EXTERNAL TABLE raw.hdfs_payments_e_krylova (
	client_id text,
	payment_id text,
	payment_date text,
	currency text,
	amount text,
	payment_method text
)
LOCATION ('pxf://user/e.krylova/study_project_b/payments/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

-- Шаг 2: Проверка данных
select * from raw.hdfs_payments_e_krylova limit 10 ;

-----------------------------------------------------------------------------------------------------------------
-- 5. Импорт данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------

-- Шаг 1: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists raw.hdfs_transactions_e_krylova;
CREATE EXTERNAL TABLE raw.hdfs_transactions_e_krylova (
	client_id text,
	transaction_id text,
	transaction_date text,
	transaction_type text,
	account_number text,
	currency text,
	amount text,
	record_saved_at text
)
LOCATION ('pxf://user/e.krylova/study_project_b/transactions/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

-- Шаг 2: Проверка данных
select * from raw.hdfs_transactions_e_krylova limit 10 ;


commit;











