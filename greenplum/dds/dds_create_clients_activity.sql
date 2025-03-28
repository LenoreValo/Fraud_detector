begin transaction;

---------------------------------------------------------------------------------------------------------------
-- 2. Таблица данных с информацией об активностях клиентов
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы типов активностей пользователей
drop table if exists dds.clients_activity_types_e_krylova;
create table dds.clients_activity_types_e_krylova (
	activity_type_id SERIAL,
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
commit;