begin transaction;

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










