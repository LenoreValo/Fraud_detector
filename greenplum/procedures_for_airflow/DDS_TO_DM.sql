-- DDS to DM: Создание витрин данных
---------------------------------------------------------------------------------------------------------------
-- 1. Витрина данных с информацией о клиентах и их активностях и логинах
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dm.load_clients_activity_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Шаг 1: Создание таблицы клиентов в слое dds
	drop table if exists dm.clients_activity_logins_e_krylova;
	create table dm.clients_activity_logins_e_krylova (
		client_id int,
		client_first_name varchar(50),
		client_last_name varchar(50),
		client_email varchar(255),
		client_phone varchar(30),
		client_address text,
		client_birthday DATE,
		activity_date TIMESTAMPTZ,
		activity_type_id int,
		activity_type_name varchar(30),
		activity_location varchar(255), 
		ip_address_activity INET,
		activity_device varchar(255), 
		login_date TIMESTAMPTZ,
		ip_address_login  INET,
		login_location varchar(255),
		login_device varchar(255)
		)
	with (
		appendoptimized = true,
		compresstype = zstd,
		compresslevel = 1,
		orientation = column
		)
	DISTRIBUTED by (client_id)
	;

    -- Вставка данных в таблицу 
    insert into dm.clients_activity_logins_e_krylova
	select 
		a.client_id as client_id,
		c.client_first_name as client_first_name,
		c.client_last_name as client_last_name,
		c.client_email as client_email,
		c.client_phone as client_phone,
		c.client_address as client_address,
		c.client_birthday as client_birthday,
		a.activity_date as activity_date,
		a.activity_type as activity_type_id,
		t.activity_type_name as activity_type_name,
		a.activity_location as activity_location, 
		a.ip_address as ip_address_activity,
		a.device as activity_device, 
		l.login_date as login_date,
		l.ip_address as ip_address_login,
		l."location" as login_location,
		l.device as login_device
	from dds.clients_activity_e_krylova a
	join dds.clients_activity_types_e_krylova t on a.activity_type = t.activity_type_id 
	join dds.clients_e_krylova c on c.client_id = a.client_id 
	join dds.logins_e_krylova l on a.client_id = l.client_id 
	;
END;
$procedure$
;
-----------------------------------------------------------------------------------------------------------------
-- 2. Витрина данных с информацией о транзакциях
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dm.load_transactions_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    drop table if exists dm.transactions_e_krylova;
	create table dm.transactions_e_krylova (
		client_id int,
		client_first_name varchar(50),
		client_last_name varchar(50),
		client_email varchar(255),
		client_phone varchar(30),
		client_address text,
		client_birthday DATE,
		account_number varchar(100),
		transaction_id int,
		transaction_date TIMESTAMPTZ,
		transaction_type_id int,
		transaction_type_name varchar(50),
		currency_id int,
		currency_name varchar(3),
		total_amount numeric(15,2)
		)
	with (
		appendoptimized = true,
		compresstype = zstd,
		compresslevel = 1,
		orientation = column
		)
	DISTRIBUTED by (client_id)
	;

    -- Вставка данных в таблицу 
	insert into dm.transactions_e_krylova
	select
		t.client_id as client_id,
		cl.client_first_name as client_first_name,
		cl.client_last_name  as client_last_name,
		cl.client_email as client_email,
		cl.client_phone as client_phone,
		cl.client_address as client_address,
		cl.client_birthday as client_birthday,
		t.account_number as account_number,
		t.transaction_id as transaction_id,
		t.transaction_date as transaction_date,
		t.transaction_type as transaction_type_id,
		tt.transaction_type_name as transaction_type_name,
		t.currency as currency_id,
		c.currency_name as currency_name,
		t.amount as total_amount
	from dds.transactions_e_krylova t 
	join dds.clients_e_krylova cl on cl.client_id = t.client_id 
	join dds.currency_e_krylova c on t.currency = c.currency_id 
	join dds.transaction_types_e_krylova tt on tt.transaction_type_id = t.transaction_type 
	;
END;
$procedure$
;
-----------------------------------------------------------------------------------------------------------------
-- 3. Витрина данных с информацией о платежах
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dm.load_payments_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    drop table if exists dm.payments_e_krylova;
	create table dm.payments_e_krylova (
		client_id int,
		client_first_name varchar(50),
		client_last_name varchar(50),
		client_email varchar(255),
		client_phone varchar(30),
		client_address text,
		client_birthday DATE,
		payment_id int,
		account_number varchar(100),
		payment_date TIMESTAMPTZ,
		currency_id int,
		currency_name varchar(3),
		total_amount numeric(15,2),
		payment_method_id int,
		payment_method_name varchar(50)
		)
	with (
		appendoptimized = true,
		compresstype = zstd,
		compresslevel = 1,
		orientation = column
		)
	DISTRIBUTED by (client_id)
	;

    -- Вставка данных в таблицу 
	insert into dm.payments_e_krylova
	select 
		p.client_id as client_id,
		cl.client_first_name as client_first_name,
		cl.client_last_name  as client_last_name,
		cl.client_email as client_email,
		cl.client_phone as client_phone,
		cl.client_address as client_address,
		cl.client_birthday as client_birthday,
		p.payment_id as payment_id,
		t.account_number as account_number,
		p.payment_date as payment_date,
		p.currency as currency_id,
		c.currency_name as currency_name,
		p.amount as total_amount,
		p.payment_method as payment_method_id,
		pm.payment_method_name as payment_method_name
	from dds.payments_e_krylova p
	join dds.clients_e_krylova cl on cl.client_id = p.client_id 
	join dds.currency_e_krylova c on c.currency_id = p.currency 
	join dds.payment_methods_e_krylova pm on pm.payment_method_id = p.payment_method 
	join dds.transactions_e_krylova t on p.client_id = t.client_id 
	;
END;
$procedure$
;










