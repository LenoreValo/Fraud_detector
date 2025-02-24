-- Начало транзакции
begin transaction;

create schema if not exists raw;

---------------------------------------------------------------------------------------------------------------
-- 1. Импорт данных с информацией о клиентах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы клиентов в слое raw
drop table if exists raw.clients_e_krylova;
create table raw.clients_e_krylova (
	client_id numeric,
	client_first_name text,
	client_last_name text,
	client_email text,
	client_phone text,
	client_address text,
	client_birthday timestamp
) DISTRIBUTED replicated
;

-- Шаг 2: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists hdfs_clients_e_krylova;
CREATE EXTERNAL TABLE hdfs_clients_e_krylova (
	client_id numeric,
	client_first_name text,
	client_last_name text,
	client_email text,
	client_phone text,
	client_address text,
	client_birthday timestamp
)
LOCATION ('pxf://user/e.krylova/study_project_b/clients/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

select * from hdfs_clients_e_krylova order by client_id limit 20 ;

-- Шаг 3: Импорт данных из всех CSV-файлов в Greenplum
INSERT INTO raw.clients_e_krylova
SELECT * FROM hdfs_clients_e_krylova;

-- Шаг 4: Проверка данных
SELECT * FROM raw.clients_e_krylova LIMIT 20;

-----------------------------------------------------------------------------------------------------------------
-- 2. Импорт данных с информацией об активности клиентов
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы активности в слое raw
drop table if exists raw.clients_activity_e_krylova;
create table raw.clients_activity_e_krylova (
	client_id numeric,
	activity_date timestamp,
	activity_type text,
	activity_location text,
	ip_address text,
	device text
) distributed by (client_id)
;

-- Шаг 2: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists hdfs_clients_activity_e_krylova;
CREATE EXTERNAL TABLE hdfs_clients_activity_e_krylova (
	client_id numeric,
	activity_date timestamp,
	activity_type text,
	activity_location text,
	ip_address text,
	device text
)
LOCATION ('pxf://user/e.krylova/study_project_b/clients_activity/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

select * from hdfs_clients_activity_e_krylova limit 20 ;

-- Шаг 3: Импорт данных из всех CSV-файлов в Greenplum
INSERT INTO raw.clients_activity_e_krylova
SELECT * FROM hdfs_clients_activity_e_krylova;

-- Шаг 4: Проверка данных
SELECT * FROM raw.clients_activity_e_krylova LIMIT 20;

-----------------------------------------------------------------------------------------------------------------
-- 3. Импорт данных с информацией о логинах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы логинов в слое raw
drop table if exists raw.logins_e_krylova;
create table raw.logins_e_krylova (
	client_id numeric,
	login_date timestamp,
	ip_address text,
	location text,
	device text
) distributed by (client_id)
;

-- Шаг 2: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists hdfs_logins_e_krylova;
CREATE EXTERNAL TABLE hdfs_logins_e_krylova (
	client_id numeric,
	login_date timestamp,
	ip_address text,
	location text,
	device text
)
LOCATION ('pxf://user/e.krylova/study_project_b/logins/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

select * from hdfs_logins_e_krylova limit 20 ;

-- Шаг 3: Импорт данных из всех CSV-файлов в Greenplum
INSERT INTO raw.logins_e_krylova
SELECT * FROM hdfs_logins_e_krylova;

-- Шаг 4: Проверка данных
SELECT * FROM raw.logins_e_krylova LIMIT 20;

-----------------------------------------------------------------------------------------------------------------
-- 4. Импорт данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы платежей в слое raw
drop table if exists raw.payments_e_krylova;
create table raw.payments_e_krylova (
	client_id numeric,
	payment_id numeric,
	payment_date timestamp,
	currency text,
	amount numeric,
	payment_method text
) distributed by (client_id)
;

-- Шаг 2: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists hdfs_payments_e_krylova;
CREATE EXTERNAL TABLE hdfs_payments_e_krylova (
	client_id numeric,
	payment_id numeric,
	payment_date timestamp,
	currency text,
	amount numeric,
	payment_method text
)
LOCATION ('pxf://user/e.krylova/study_project_b/payments/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

select * from hdfs_payments_e_krylova limit 20 ;

-- Шаг 3: Импорт данных из всех CSV-файлов в Greenplum
INSERT INTO raw.payments_e_krylova
SELECT * FROM hdfs_payments_e_krylova;

-- Шаг 4: Проверка данных
SELECT * FROM raw.payments_e_krylova LIMIT 20;

-----------------------------------------------------------------------------------------------------------------
-- 5. Импорт данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------

-- Шаг 1: Создание таблицы транзакций в слое raw
drop table if exists raw.transactions_e_krylova;
create table raw.transactions_e_krylova (
	client_id numeric,
	transaction_id numeric,
	transaction_date timestamp,
	transaction_type text,
	account_number text,
	currency text,
	amount numeric,
	record_saved_at text
) distributed by (client_id)
;

-- Шаг 2: Создание внешней таблицы для всех CSV-файлов в HDFS
drop foreign table if exists hdfs_transactions_e_krylova;
CREATE EXTERNAL TABLE hdfs_transactions_e_krylova (
	client_id numeric,
	transaction_id numeric,
	transaction_date timestamp,
	transaction_type text,
	account_number text,
	currency text,
	amount numeric,
	record_saved_at text
)
LOCATION ('pxf://user/e.krylova/study_project_b/transactions/csv_files/*.csv?PROFILE=hdfs:text')
FORMAT 'CSV' (header=true);

select * from hdfs_transactions_e_krylova limit 20 ;

-- Шаг 3: Импорт данных из всех CSV-файлов в Greenplum
INSERT INTO raw.transactions_e_krylova
SELECT * FROM hdfs_transactions_e_krylova;

-- Шаг 4: Проверка данных
SELECT * FROM raw.transactions_e_krylova LIMIT 20;

commit;











