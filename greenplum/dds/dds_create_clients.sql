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

commit;