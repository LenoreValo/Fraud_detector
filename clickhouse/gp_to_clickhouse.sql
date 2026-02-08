-- Мигрируем витрины данных из GreenPlum в ClickHouse

-- Используем нашу БД
USE wave18_team_b;

---------------------------------------------------------------------------------------------------------------
-- 1. Витрина данных с информацией о клиентах и их активностях и логинах
----------------------------------------------------------------------------------------------------------------
-- Создаем внешнюю таблицу в ClickHouse:
drop table if exists gp_clients_activity_e_krylova;
create table gp_clients_activity_e_krylova(
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
	ip_address_login  String,
	login_location String,
	login_device String
) ENGINE = PostgreSQL('172.17.1.32:5432', 'wave18_team_b', 'clients_activity_logins_e_krylova', 'gpadmin', '', 'dm')
;

-- Создание таблицы в ClickHouse
drop table if exists clients_activity_e_krylova;
CREATE TABLE clients_activity_e_krylova
(
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
	ip_address_login  IPv4,
	login_location String,
	login_device String
) ENGINE = MergeTree()
ORDER BY client_id;

-- Скопируем данные в локальную таблицу ClickHouse:
INSERT INTO clients_activity_e_krylova
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
	login_date ,
	CASE
        WHEN match(ip_address_login, '^(\d{1,3}\.){3}\d{1,3}$') AND 
             toUInt32OrNull(splitByString('.', ip_address_login)[1]) <= 255 AND
             toUInt32OrNull(splitByString('.', ip_address_login)[2]) <= 255 AND
             toUInt32OrNull(splitByString('.', ip_address_login)[3]) <= 255 AND
             toUInt32OrNull(splitByString('.', ip_address_login)[4]) <= 255
        THEN toIPv4(ip_address_login)
        ELSE NULL
    END AS ip_address_login,
	login_location ,
	login_device
FROM gp_clients_activity_e_krylova;

-- Проверка
SELECT * from clients_activity_e_krylova limit 20;

-----------------------------------------------------------------------------------------------------------------
-- 2. Витрина данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------
-- Создаем внешнюю таблицу в ClickHouse:
drop table if exists gp_transactions_e_krylova;
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
) ENGINE = PostgreSQL('172.17.1.32:5432', 'wave18_team_b', 'transactions_e_krylova', 'gpadmin', '', 'dm')
;

-- Создание таблицы в ClickHouse
drop table if exists transactions_e_krylova;
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

-- Скопируем данные в локальную таблицу ClickHouse:
INSERT INTO transactions_e_krylova
SELECT * FROM gp_transactions_e_krylova;

-- Проверка
SELECT * from transactions_e_krylova limit 20;

-----------------------------------------------------------------------------------------------------------------
-- 3. Витрина данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
-- Создаем внешнюю таблицу в ClickHouse:
drop table if exists gp_payments_e_krylova;
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
) ENGINE = PostgreSQL('172.17.1.32:5432', 'wave18_team_b', 'payments_e_krylova', 'gpadmin', '', 'dm')
;

-- Создание таблицы в ClickHouse
drop table if exists payments_e_krylova;
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

-- Скопируем данные в локальную таблицу ClickHouse:
INSERT INTO payments_e_krylova
SELECT * FROM gp_payments_e_krylova;

-- Проверка
SELECT * from payments_e_krylova limit 20;



