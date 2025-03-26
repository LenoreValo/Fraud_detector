-- Формирование DDS слоя: создание таблиц
begin transaction;

create schema if not exists dds;

---------------------------------------------------------------------------------------------------------------
-- 1. Таблица данных с информацией о клиентах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы клиентов в слое dds
drop table if exists dds.clients_e_krylova;
create table dds.clients_e_krylova (
	client_id int not null,
	client_first_name varchar(50) not null,
	client_last_name varchar(50) not null,
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

---------------------------------------------------------------------------------------------------------------
-- 2. Таблица данных с информацией об активностях клиентов
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы типов активностей пользователей
drop table if exists dds.clients_activity_types_e_krylova;
create table dds.clients_activity_types_e_krylova (
	activity_type_id SERIAL primary key,
	activity_type_name varchar(50) not null
)
distributed by (activity_type_id)
;

-- Шаг 2: Создание таблицы активностей в слое dds
drop table if exists dds.clients_activity_e_krylova;
create table dds.clients_activity_e_krylova (
	client_id int not null,
	activity_date TIMESTAMPTZ not null,
	activity_type int,
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
distributed by (client_id, activity_date)
;

-----------------------------------------------------------------------------------------------------------------
-- 3. Таблица данных с информацией о логинах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы логинов в слое dds
drop table if exists dds.logins_e_krylova;
create table dds.logins_e_krylova (
	client_id int not null,
	login_date TIMESTAMPTZ not null,
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
distributed by (client_id, login_date)
;

-----------------------------------------------------------------------------------------------------------------
-- 4. Таблица данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы валют
drop table if exists dds.currency_e_krylova;
create table dds.currency_e_krylova (
	currency_id SERIAL primary key,
	currency_name varchar(3) not null
)
distributed by (currency_id)
;

-- Шаг 2: Создание таблицы методов платежей
drop table if exists dds.payment_methods_e_krylova;
create table dds.payment_methods_e_krylova (
	payment_method_id SERIAL,
	payment_method_name varchar(50) not null
)
distributed by (payment_method_id)
;

-- Шаг 3: Создание таблицы платежей в слое dds
drop table if exists dds.payments_e_krylova;
create table dds.payments_e_krylova (
	payment_id int not null,
	client_id int not null,
	payment_date timestamp,
	currency int,
	amount numeric(15,2),
	payment_method int
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (payment_id)
;

-----------------------------------------------------------------------------------------------------------------
-- 5. Таблица данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы типов транзакций
drop table if exists dds.transaction_types_e_krylova;
create table dds.transaction_types_e_krylova (
	transaction_type_id SERIAL,
	transaction_type_name varchar(50) not null
)
distributed by (transaction_type_id);

-- Шаг 2: Создание таблицы транзакций в слое dds
drop table if exists dds.transactions_e_krylova;
create table dds.transactions_e_krylova (
	transaction_id int not null,
	client_id int not null,
	transaction_date TIMESTAMPTZ,
	transaction_type int,
	account_number varchar(100),
	currency int,
	amount numeric(15,2)
)
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (transaction_id)
;

commit;










