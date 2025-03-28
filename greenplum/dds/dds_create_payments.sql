begin transaction;

-----------------------------------------------------------------------------------------------------------------
-- 4. Таблица данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы валют
drop table if exists dds.currency_e_krylova;
create table dds.currency_e_krylova (
	currency_id SERIAL,
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

commit;