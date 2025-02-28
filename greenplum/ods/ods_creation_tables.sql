-- Формирование ODS слоя
begin transaction;

create schema if not exists ods;

---------------------------------------------------------------------------------------------------------------
-- 1. Создание таблицы клиентов в слое ods
----------------------------------------------------------------------------------------------------------------
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
-----------------------------------------------------------------------------------------------------------------
-- 2. Создание таблицы активности в слое ods
----------------------------------------------------------------------------------------------------------------
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
-----------------------------------------------------------------------------------------------------------------
-- 3. Создание таблицы логинов в слое ods
----------------------------------------------------------------------------------------------------------------
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
-----------------------------------------------------------------------------------------------------------------
-- 4. Создание таблицы платежей в слое ods
----------------------------------------------------------------------------------------------------------------
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
-----------------------------------------------------------------------------------------------------------------
-- 5. Создание таблицы транзакций в слое ods
----------------------------------------------------------------------------------------------------------------
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


commit;











