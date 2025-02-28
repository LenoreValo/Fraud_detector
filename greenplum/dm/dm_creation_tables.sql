-- DDS to DM: Создание витрин данных
begin transaction;

create schema if not exists dm;
---------------------------------------------------------------------------------------------------------------
-- 1. Витрина данных с информацией о клиентах и их активностях и логинах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы клиентов в слое dds
drop table if exists dm.clients_activity_logins_e_krylova;
create table dm.clients_activity_logins_e_krylova (
	client_id int,
	client_first_name varchar(50),
	client_last_name varchar(50),
	client_email varchar(255),
	client_phone varchar(30),
	client_address text,
	client_birthday DATE,
	activity_date TIMESTAMPTZ,
	activity_type_id int,
	activity_type_name varchar(30),
	activity_location varchar(255), 
	ip_address_activity INET,
	activity_device varchar(255), 
	login_date TIMESTAMPTZ,
	ip_address_login  INET,
	login_location varchar(255),
	login_device varchar(255)
	)
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
DISTRIBUTED by (client_id)
;

-----------------------------------------------------------------------------------------------------------------
-- 2. Витрина данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------
drop table if exists dm.transactions_e_krylova;
create table dm.transactions_e_krylova (
	client_id int,
	client_first_name varchar(50),
	client_last_name varchar(50),
	client_email varchar(255),
	client_phone varchar(30),
	client_address text,
	client_birthday DATE,
	account_number varchar(100),
	transaction_id int,
	transaction_date TIMESTAMPTZ,
	transaction_type_id int,
	transaction_type_name varchar(50),
	currency_id int,
	currency_name varchar(3),
	total_amount numeric(15,2)
	)
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
DISTRIBUTED by (client_id)
;

-----------------------------------------------------------------------------------------------------------------
-- 3. Витрина данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
drop table if exists dm.payments_e_krylova;
create table dm.payments_e_krylova (
	client_id int,
	client_first_name varchar(50),
	client_last_name varchar(50),
	client_email varchar(255),
	client_phone varchar(30),
	client_address text,
	client_birthday DATE,
	payment_id int,
	account_number varchar(100),
	payment_date TIMESTAMPTZ,
	currency_id int,
	currency_name varchar(3),
	total_amount numeric(15,2),
	payment_method_id int,
	payment_method_name varchar(50)
	)
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
DISTRIBUTED by (client_id)
;


commit;









