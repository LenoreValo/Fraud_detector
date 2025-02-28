-- Процедуры для вставки данных из RAW в ODS
begin transaction;
--------------------------------------------------------------------------------------------
-- 1. Вставка данных в таблицу с информацией о клиентах
--------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ods.transform_load_clients_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE ods.clients_e_krylova;

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
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE ods.clients_activity_e_krylova;

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
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE ods.logins_e_krylova;

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
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE ods.payments_e_krylova;

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
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE ods.transactions_e_krylova;

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
--------------------------------------------------------------------------------------------
-- 6. Вызовы процедур
--------------------------------------------------------------------------------------------
call ods.transform_load_clients_e_krylova();
call ods.transform_load_clients_activity_e_krylova();
call ods.transform_load_logins_e_krylova();
call ods.transform_load_payments_e_krylova();
call ods.transform_load_transactions_e_krylova();

-- Проверка
SELECT * FROM ods.clients_e_krylova LIMIT 10;;
SELECT * FROM ods.clients_activity_e_krylova LIMIT 20;;
SELECT * FROM ods.logins_e_krylova LIMIT 10;;
SELECT * FROM ods.payments_e_krylova LIMIT 10;
SELECT * FROM ods.transactions_e_krylova LIMIT 10;

commit;



























