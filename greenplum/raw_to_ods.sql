-- Формирование ODS слоя
begin transaction;

create schema if not exists ods;

---------------------------------------------------------------------------------------------------------------
-- 1. Импорт данных с информацией о клиентах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы клиентов в слое ods
drop table if exists ods.clients_e_krylova;
create table ods.clients_e_krylova (
	client_id float4 not null primary key,
	client_first_name varchar(50),
	client_last_name varchar(50),
	client_email varchar(255),
	client_phone varchar(30),
	client_address text,
	client_birthday DATE
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
DISTRIBUTED replicated
;

-- Шаг 2: Загрузка данных из raw
INSERT INTO ods.clients_e_krylova
SELECT  distinct *
FROM raw.hdfs_clients_e_krylova
where client_first_name not like '%NaN%' and client_last_name not like '%NaN%';

-- Шаг 3: Проверка данных
SELECT * FROM ods.clients_e_krylova LIMIT 10;

-----------------------------------------------------------------------------------------------------------------
-- 2. Импорт данных с информацией об активности клиентов
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы активности в слое ods
drop table if exists ods.clients_activity_e_krylova;
create table ods.clients_activity_e_krylova (
	client_id float4,
	activity_date TIMESTAMPTZ,
	activity_type varchar(50),
	activity_location varchar(255),
	ip_address INET,
	device varchar(255)
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (client_id)
;

-- Шаг 2: Загрузка данных из raw
INSERT INTO ods.clients_activity_e_krylova
SELECT distinct
	ods.safe_to_float_with_check(client_id) AS client_id,
	ods.safe_to_timestamptz_with_check(activity_date) AS activity_date,
	activity_type,
	activity_location,
	ods.safe_to_inet_with_check(ip_address) as ip_address, 
	device
FROM raw.hdfs_clients_activity_e_krylova;

-- Шаг 3: Проверка данных
SELECT * FROM ods.clients_activity_e_krylova LIMIT 20;

-----------------------------------------------------------------------------------------------------------------
-- 3. Импорт данных с информацией о логинах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы логинов в слое ods
drop table if exists ods.logins_e_krylova;
create table ods.logins_e_krylova (
	client_id float4,
	login_date TIMESTAMPTZ,
	ip_address INET,
	location varchar(255),
	device varchar(255)
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (client_id)
;

-- Шаг 2: Загрузка данных из raw
INSERT INTO ods.logins_e_krylova
select distinct
	ods.safe_to_float_with_check(client_id) AS client_id,
	ods.safe_to_timestamptz_with_check(login_date) AS login_date,
	ods.safe_to_inet_with_check(ip_address) as ip_address,
	location,
	device
FROM raw.hdfs_logins_e_krylova
;

-- Шаг 3: Проверка данных
SELECT * FROM ods.logins_e_krylova LIMIT 10;

-----------------------------------------------------------------------------------------------------------------
-- 4. Импорт данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы платежей в слое ods
drop table if exists ods.payments_e_krylova;
create table ods.payments_e_krylova (
	client_id float4 ,
	payment_id float4,
	payment_date timestamp,
	currency varchar(10),
	amount numeric(15,2),
	payment_method varchar(50)
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (payment_id)
;

-- Шаг 2: Загрузка данных из raw
INSERT INTO ods.payments_e_krylova
SELECT distinct
	ods.safe_to_float_with_check(client_id) AS client_id,
	ods.safe_to_float_with_check(payment_id) AS payment_id,
	ods.safe_to_timestamptz_with_check(payment_date) AS payment_date,
	currency,
	ods.safe_to_numeric_with_check(amount) as amount,
	payment_method
FROM raw.hdfs_payments_e_krylova;

-- Шаг 3: Проверка данных
SELECT * FROM ods.payments_e_krylova LIMIT 10;

-----------------------------------------------------------------------------------------------------------------
-- 5. Импорт данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------

-- Шаг 1: Создание таблицы транзакций в слое ods
drop table if exists ods.transactions_e_krylova;
create table ods.transactions_e_krylova (
	client_id float4,
	transaction_id float4,
	transaction_date TIMESTAMPTZ,
	transaction_type varchar(50),
	account_number varchar(100),
	currency varchar(10),
	amount numeric(15,2),
	record_saved_at varchar(50)
)
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (transaction_id)
;

-- Шаг 2: Загрузка данных из raw
INSERT INTO ods.transactions_e_krylova
select distinct
	ods.safe_to_float_with_check(client_id) AS client_id,
	ods.safe_to_float_with_check(transaction_id) AS transaction_id,
	ods.safe_to_timestamptz_with_check(transaction_date) AS transaction_date,
	transaction_type,
	account_number,
	currency,
	ods.safe_to_numeric_with_check(amount) as amount,
	record_saved_at
FROM raw.hdfs_transactions_e_krylova;

-- Шаг 3: Проверка данных
SELECT * FROM ods.transactions_e_krylova LIMIT 10;

commit;






