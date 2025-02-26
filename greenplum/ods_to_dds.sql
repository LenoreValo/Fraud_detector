-- Формирование DDS слоя
begin transaction;

create schema if not exists dds;
---------------------------------------------------------------------------------------------------------------
-- 1. Загрузка данных с информацией о клиентах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы клиентов в слое dds
drop table if exists dds.clients_e_krylova;
create table dds.clients_e_krylova (
	client_id int not null primary key,
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
-- Шаг 2: вызов процедуры для вставки данных
CALL dds.transform_and_load_clients_e_krylova();

-- Шаг 3: проверка данных
select * from dds.clients_e_krylova limit 10;

---------------------------------------------------------------------------------------------------------------
-- 2. Загрузка данных с информацией об активностях клиентов
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы типов активностей пользователей
drop table if exists dds.clients_activity_types_e_krylova;
create table dds.clients_activity_types_e_krylova (
	activity_type_id SERIAL primary key,
	activity_type_name varchar(50) not null
)
distributed by (activity_type_id)
;
-- вызов процедуры для вставки данных
call dds.transform_and_load_activity_types_e_krylova();
-- Проверка данных
select * from dds.clients_activity_types_e_krylova;

-- Шаг 2: Создание таблицы активностей в слое dds
drop table if exists dds.clients_activity_e_krylova;
create table dds.clients_activity_e_krylova (
	client_id int not null,
	activity_date TIMESTAMPTZ not null,
	activity_type int,
	activity_location varchar(255),
	ip_address INET,
	device varchar(255),
	primary key (client_id, activity_date),
	FOREIGN KEY (activity_type) REFERENCES dds.clients_activity_types_e_krylova(activity_type_id),
	FOREIGN KEY (client_id) REFERENCES dds.clients_e_krylova(client_id)
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (client_id, activity_date)
;
-- Шаг 3: Вставка данных
call dds.transform_and_load_clients_activity_e_krylova();

-- Шаг 4: Проверка данных
SELECT * FROM dds.clients_activity_e_krylova LIMIT 20;

-----------------------------------------------------------------------------------------------------------------
-- 3. Загрузка данных с информацией о логинах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы логинов в слое dds
drop table if exists dds.logins_e_krylova;
create table dds.logins_e_krylova (
	client_id int not null,
	login_date TIMESTAMPTZ not null,
	ip_address INET,
	location varchar(255),
	device varchar(255),
	primary key (client_id, login_date),
	FOREIGN KEY (client_id) REFERENCES dds.clients_e_krylova(client_id)
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (client_id, login_date)
;
-- Шаг 2: Вставка данных
call dds.transform_and_load_logins_e_krylova();
-- Шаг 3: Проверка данных
SELECT * FROM dds.logins_e_krylova LIMIT 20;

-----------------------------------------------------------------------------------------------------------------
-- 4. Загрузка данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы валют
drop table if exists dds.currency_e_krylova;
create table dds.currency_e_krylova (
	currency_id SERIAL primary key,
	currency_name varchar(3) not null
)
distributed by (currency_id)
;
-- вставка данных
call dds.transform_and_load_currency_e_krylova();
-- проверка
select * from dds.currency_e_krylova;

-- Шаг 2: Создание таблицы методов платежей
drop table if exists dds.payment_methods_e_krylova;
create table dds.payment_methods_e_krylova (
	payment_method_id SERIAL primary key,
	payment_method_name varchar(50) not null
)
distributed by (payment_method_id)
;
-- вставка данных
call dds.transform_and_load_payment_methods_e_krylova();
-- проверка
select * from dds.payment_methods_e_krylova;

-- Шаг 3: Создание таблицы платежей в слое dds
drop table if exists dds.payments_e_krylova;
create table dds.payments_e_krylova (
	client_id int not null,
	payment_id int not null primary key,
	payment_date timestamp,
	currency int,
	amount numeric(15,2),
	payment_method int,
	FOREIGN KEY (client_id) REFERENCES dds.clients_e_krylova(client_id),
	FOREIGN KEY (currency) REFERENCES dds.currency_e_krylova(currency_id),
	FOREIGN KEY (payment_method) REFERENCES dds.payment_methods_e_krylova(payment_method_id)
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
distributed by (payment_id)
;

-- Шаг 4: Вставка данных
 

















