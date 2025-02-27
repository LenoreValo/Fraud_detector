-- Процедуры для вставки данных из ODS в DDS
begin transaction;
-- 1. Вставка данных в таблицу с информацией о клиентах
CREATE OR REPLACE PROCEDURE dds.transform_and_load_clients_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE dds.clients_e_krylova;

    -- Вставка данных в таблицу 
    INSERT INTO dds.clients_e_krylova 
    SELECT 
        cast(client_id as INT) as client_id,
		client_first_name,
		client_last_name,
		NULLIF(client_email, '\NaN\') as client_email,
		NULLIF(client_phone, '\NaN\') as client_phone,
		NULLIF(client_address,'\NaN\') as client_address,
		client_birthday
    FROM 
        ods.clients_e_krylova
    WHERE 
        client_id IS NOT NULL;
END;
$procedure$
;
------------------------------------------------------------------------------------------------------
-- 2. Вставка данных в таблицу с информацией о типах активностей
CREATE OR REPLACE PROCEDURE dds.transform_and_load_activity_types_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE dds.clients_activity_types_e_krylova;

    -- Вставка данных в таблицу
   insert into dds.clients_activity_types_e_krylova (activity_type_name)
		select 
			activity_type
		from ods.clients_activity_e_krylova
		WHERE LOWER(activity_type) NOT IN ('\nan\', 'activity_type')
		group by activity_type;
END;
$procedure$
;
------------------------------------------------------------------------------------------------------
-- 3. Вставка данных в таблицу с информацией об активностях клиентов
CREATE OR REPLACE PROCEDURE dds.transform_and_load_clients_activity_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы
    TRUNCATE TABLE dds.clients_activity_e_krylova;

    -- Вставка данных в таблицу 
    INSERT INTO dds.clients_activity_e_krylova 
    SELECT distinct on (client_id, activity_date)
		cast(a.client_id as INT) as client_id,
		a.activity_date as activity_date,
		t.activity_type_id as activity_type,
		NULLIF(a.activity_location, '\NaN\') as activity_location,
		a.ip_address as ip_address,
		NULLIF(a.device, '\NaN\') as device
    FROM ods.clients_activity_e_krylova as a
	join dds.clients_activity_types_e_krylova as t on a.activity_type = t.activity_type_name
    WHERE 
        client_id IS NOT NULL and activity_date IS NOT NULL and activity_type IS NOT NULL;
	--ORDER BY client_id, activity_date, activity_type;
END;
$procedure$
;

------------------------------------------------------------------------------------------------------
-- 4. Вставка данных в таблицу с информацией о логинах клиентов
CREATE OR REPLACE PROCEDURE dds.transform_and_load_logins_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE dds.logins_e_krylova;

    -- Вставка данных в таблицу 
    INSERT INTO dds.logins_e_krylova 
    SELECT distinct on (client_id, login_date)
		cast(a.client_id as INT) as client_id,
		login_date TIMESTAMPTZ,
		ip_address INET,
		NULLIF(location, '\NaN\') as location,
		NULLIF(device, '\NaN\') as device
    FROM 
        ods.logins_e_krylova as a
    WHERE 
        client_id IS NOT NULL and login_date IS NOT NULL;
END;
$procedure$
;

------------------------------------------------------------------------------------------------------
-- 5. Вставка данных в таблицу с информацией о типах валют
CREATE OR REPLACE PROCEDURE dds.transform_and_load_currency_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы
    TRUNCATE TABLE dds.currency_e_krylova;

    -- Вставка данных в таблицу 
   insert into dds.currency_e_krylova (currency_name)
		select 
			currency
		from ods.payments_e_krylova
		WHERE LOWER(currency) NOT IN ('\nan\', 'currency')
		group by currency;
END;
$procedure$
;
------------------------------------------------------------------------------------------------------
-- 6. Вставка данных в таблицу с информацией о методах платежей
CREATE OR REPLACE PROCEDURE dds.transform_and_load_payment_methods_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE dds.payment_methods_e_krylova;

    -- Вставка данных в таблицу 
   insert into dds.payment_methods_e_krylova (payment_method_name)
		select 
			payment_method
		from ods.payments_e_krylova
		WHERE LOWER(payment_method) NOT IN ('\nan\', 'payment_method')
		group by payment_method;
END;
$procedure$
;
------------------------------------------------------------------------------------------------------
-- 7. Вставка данных в таблицу с информацией о платежах
CREATE OR REPLACE PROCEDURE dds.transform_and_load_payments_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE dds.payments_e_krylova;

    -- Вставка данных в таблицу 
   insert into dds.payments_e_krylova
		select distinct on (payment_id)
			cast(p.payment_id as INT) as payment_id,
			cast(p.client_id as INT) as client_id,
			payment_date,
			c.currency_id as currency,
			amount,
			m.payment_method_id as payment_method
		from ods.payments_e_krylova p
		join dds.currency_e_krylova c on p.currency = c.currency_name
		join dds.payment_methods_e_krylova m on p.payment_method = m.payment_method_name
		where payment_id is not null and payment_date between '2010-01-01' and '2026-01-01'
;
END;
$procedure$
;

------------------------------------------------------------------------------------------------------
-- 8. Вставка данных в таблицу с информацией о методах платежей
CREATE OR REPLACE PROCEDURE dds.transform_and_load_transaction_types_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE dds.transaction_types_e_krylova;

    -- Вставка данных в таблицу 
   insert into dds.transaction_types_e_krylova (transaction_type_name)
		select 
			transaction_type
		from ods.transactions_e_krylova
		WHERE LOWER(transaction_type) NOT IN ('\nan\', 'transaction_type')
		group by transaction_type;
END;
$procedure$
;

------------------------------------------------------------------------------------------------------
-- 9. Вставка данных в таблицу с информацией о транзакциях
CREATE OR REPLACE PROCEDURE dds.transform_and_load_transactions_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Удаление существующих данных из таблицы 
    TRUNCATE TABLE dds.transactions_e_krylova;

    -- Вставка данных в таблицу 
   insert into dds.transactions_e_krylova
		select distinct on (transaction_id)
				cast(t.transaction_id as INT) as transaction_id,
				cast(t.client_id as INT) as client_id,
				t.transaction_date as transaction_date,
				tt.transaction_type_id as transaction_type,
				NULLIF(account_number, '\NaN\') as account_number,
				c.currency_id as currency,
				amount
		from ods.transactions_e_krylova t
		join dds.currency_e_krylova c on t.currency = c.currency_name
		join dds.transaction_types_e_krylova tt on t.transaction_type = tt.transaction_type_name
		where transaction_id is not null and transaction_date between '2010-01-01' and '2026-01-01'
;
END;
$procedure$
;

---------------------------------------------------------------------------------------------------------------
-- 1. Загрузка данных с информацией о клиентах
----------------------------------------------------------------------------------------------------------------

CALL dds.transform_and_load_clients_e_krylova();

---------------------------------------------------------------------------------------------------------------
-- 2. Загрузка данных с информацией об активностях клиентов
----------------------------------------------------------------------------------------------------------------

call dds.transform_and_load_activity_types_e_krylova();

call dds.transform_and_load_clients_activity_e_krylova();

-----------------------------------------------------------------------------------------------------------------
-- 3. Загрузка данных с информацией о логинах
----------------------------------------------------------------------------------------------------------------

call dds.transform_and_load_logins_e_krylova();

-----------------------------------------------------------------------------------------------------------------
-- 4. Загрузка данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
call dds.transform_and_load_currency_e_krylova();

call dds.transform_and_load_payment_methods_e_krylova();

call dds.transform_and_load_payments_e_krylova();

-----------------------------------------------------------------------------------------------------------------
-- 5. Загрузка данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------
call dds.transform_and_load_transaction_types_e_krylova();

call dds.transform_and_load_transactions_e_krylova();



commit;



