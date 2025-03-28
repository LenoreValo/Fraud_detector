begin transaction;

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

commit;