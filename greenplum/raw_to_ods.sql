-- Формирование ODS слоя
begin transaction;

create schema if not exists ods;

---------------------------------------------------------------------------------------------------------------
-- 1. Импорт данных с информацией о клиентах
----------------------------------------------------------------------------------------------------------------
-- Шаг 1: Создание таблицы клиентов в слое raw
drop table if exists ods.clients_e_krylova;
create table ods.clients_e_krylova (
	client_id float4,
	client_first_name text,
	client_last_name text,
	client_email text,
	client_phone text,
	client_address text,
	client_birthday TIMESTAMPTZ
) 
with (
	appendoptimized = true,
	compresstype = zstd,
	compresslevel = 1,
	orientation = column
	)
DISTRIBUTED replicated
;

-- Шаг 2: Импорт данных из всех CSV-файлов в Greenplum
INSERT INTO ods.clients_e_krylova
SELECT  
	CASE 
        WHEN client_id ~ '^-?\d+(\.\d+)?$' THEN client_id::FLOAT4  -- Регулярное выражение для чисел
        ELSE NULL
    END AS client_id,
	client_first_name text,
	client_last_name text,
	client_email text,
	client_phone text,
	client_address text,
	CASE 
        WHEN client_birthday ~ '^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?)$' 
        THEN client_birthday::TIMESTAMPTZ
        ELSE NULL
    END AS client_birthday
FROM raw.hdfs_clients_e_krylova;

-- Шаг 3: Проверка данных
SELECT * FROM ods.clients_e_krylova LIMIT 10;




