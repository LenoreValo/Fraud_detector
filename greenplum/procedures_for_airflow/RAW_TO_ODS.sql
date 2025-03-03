-- Процедуры для вставки данных из RAW в ODS
--------------------------------------------------------------------------------------------
-- 1. Вставка данных в таблицу с информацией о клиентах
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ods.transform_load_clients_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
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

    -- Вставка данных в таблицу 
	INSERT INTO ods.clients_e_krylova
	SELECT  distinct *
	FROM raw.hdfs_clients_e_krylova
	where client_first_name not like '%NaN%' and client_last_name not like '%NaN%';
END;
$procedure$
;

-----------------------------------------------------------------------------------------------------------------
-- 2. Вставка данных с информацией об активности клиентов
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ods.transform_load_clients_activity_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
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

    -- Вставка данных в таблицу 
	INSERT INTO ods.clients_activity_e_krylova
	SELECT distinct
		ods.safe_to_float_with_check(client_id) AS client_id,
		ods.safe_to_timestamptz_with_check(activity_date) AS activity_date,
		activity_type,
		activity_location,
		ods.safe_to_inet_with_check(ip_address) as ip_address, 
		device
	FROM raw.hdfs_clients_activity_e_krylova;
END;
$procedure$
;

-----------------------------------------------------------------------------------------------------------------
-- 3. Вставка данных с информацией о логинах
----------------------------------------------------------------------------------------------------------------
CREATE OR replace PROCEDURE ods.transform_load_logins_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
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

    -- Вставка данных в таблицу 
	INSERT INTO ods.logins_e_krylova
	select distinct
		ods.safe_to_float_with_check(client_id) AS client_id,
		ods.safe_to_timestamptz_with_check(login_date) AS login_date,
		ods.safe_to_inet_with_check(ip_address) as ip_address,
		location,
		device
	FROM raw.hdfs_logins_e_krylova
	;
END;
$procedure$
;
-----------------------------------------------------------------------------------------------------------------
-- 4. Вставка данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
CREATE OR replace PROCEDURE ods.transform_load_payments_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
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

    -- Вставка данных в таблицу 
	INSERT INTO ods.payments_e_krylova
	SELECT distinct
		ods.safe_to_float_with_check(client_id) AS client_id,
		ods.safe_to_float_with_check(payment_id) AS payment_id,
		ods.safe_to_timestamptz_with_check(payment_date) AS payment_date,
		currency,
		ods.safe_to_numeric_with_check(amount) as amount,
		payment_method
	FROM raw.hdfs_payments_e_krylova;
END;
$procedure$
;

-----------------------------------------------------------------------------------------------------------------
-- 5. Вставка данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------
CREATE OR replace PROCEDURE ods.transform_load_transactions_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
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

    -- Вставка данных в таблицу 
	INSERT INTO ods.transactions_e_krylova
	select distinct
		ods.safe_to_float_with_check(client_id) AS client_id,
		ods.safe_to_float_with_check(transaction_id) AS transaction_id,
		ods.safe_to_timestamptz_with_check(transaction_date) AS transaction_date,
		transaction_type,
		account_number,
		currency,
		ods.safe_to_numeric_with_check(amount) as amount,
		record_saved_at
	FROM raw.hdfs_transactions_e_krylova;
END;
$procedure$
;



























