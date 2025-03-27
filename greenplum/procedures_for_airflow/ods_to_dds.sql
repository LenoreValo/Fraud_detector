-- Процедуры для вставки данных из ODS в DDS


CREATE OR REPLACE PROCEDURE dds.create_and_load_data_e_krylova()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	--0. Удаление таблиц, если существуют
	drop table if exists dds.clients_activity_e_krylova;
	drop table if exists dds.logins_e_krylova;
	drop table if exists dds.payments_e_krylova;
	drop table if exists dds.transactions_e_krylova;
	drop table if exists dds.transaction_types_e_krylova;
	drop table if exists dds.payment_methods_e_krylova;
	drop table if exists dds.currency_e_krylova;
	drop table if exists dds.clients_activity_types_e_krylova;
	drop table if exists dds.clients_e_krylova;
------------------------------------------------------------------------------------------------------
-- 1. Создание таблицы клиентов в слое dds
	create table dds.clients_e_krylova (
		client_id int not null primary key,
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
------------------------------------------------------------------------------------------------------
	-- 2. Создание таблицы типов активностей пользователей
	create table dds.clients_activity_types_e_krylova (
		activity_type_id SERIAL primary key,
		activity_type_name varchar(50) not null
	)
	distributed by (activity_type_id)
	;

    -- Вставка данных в таблицу
   insert into dds.clients_activity_types_e_krylova (activity_type_name)
		select 
			activity_type
		from ods.clients_activity_e_krylova
		WHERE LOWER(activity_type) NOT IN ('\nan\', 'activity_type')
		group by activity_type;
------------------------------------------------------------------------------------------------------
-- 3. Вставка данных в таблицу с информацией об активностях клиентов
create table dds.clients_activity_e_krylova (
		client_id int not null,
		activity_date TIMESTAMPTZ not null,
		activity_type int,
		activity_location varchar(255),
		ip_address INET,
		device varchar(255),
		primary key (client_id, activity_date),
		FOREIGN KEY (activity_type) REFERENCES dds.clients_activity_types_e_krylova(activity_type_id) ON DELETE CASCADE,
		FOREIGN KEY (client_id) REFERENCES dds.clients_e_krylova(client_id) ON DELETE CASCADE
	) 
	with (
		appendoptimized = true,
		compresstype = zstd,
		compresslevel = 1,
		orientation = column
		)
	distributed by (client_id, activity_date)
	;

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
        client_id IS NOT NULL and activity_date IS NOT NULL and activity_type IS NOT NULL and activity_date between '2010-01-01' and '2026-01-01';
------------------------------------------------------------------------------------------------------
-- 4. Вставка данных в таблицу с информацией о логинах клиентов
	create table dds.logins_e_krylova (
		client_id int not null,
		login_date TIMESTAMPTZ not null,
		ip_address INET,
		location varchar(255),
		device varchar(255),
		primary key (client_id, login_date),
		FOREIGN KEY (client_id) REFERENCES dds.clients_e_krylova(client_id) ON DELETE CASCADE
	) 
	with (
		appendoptimized = true,
		compresstype = zstd,
		compresslevel = 1,
		orientation = column
		)
	distributed by (client_id, login_date)
	;

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
        client_id IS NOT NULL and login_date IS NOT NULL and login_date between '2010-01-01' and '2026-01-01';
------------------------------------------------------------------------------------------------------
-- 5. Вставка данных в таблицу с информацией о типах валют
	create table dds.currency_e_krylova (
		currency_id SERIAL primary key,
		currency_name varchar(3) not null
	)
	distributed by (currency_id)
	;

    -- Вставка данных в таблицу 
   insert into dds.currency_e_krylova (currency_name)
		select 
			currency
		from ods.payments_e_krylova
		WHERE LOWER(currency) NOT IN ('\nan\', 'currency')
		group by currency;
------------------------------------------------------------------------------------------------------
-- 6. Вставка данных в таблицу с информацией о методах платежей
	create table dds.payment_methods_e_krylova (
			payment_method_id SERIAL primary key,
			payment_method_name varchar(50) not null
		)
		distributed by (payment_method_id)
		;
	
	    -- Вставка данных в таблицу 
	   insert into dds.payment_methods_e_krylova (payment_method_name)
			select 
				payment_method
			from ods.payments_e_krylova
			WHERE LOWER(payment_method) NOT IN ('\nan\', 'payment_method')
			group by payment_method;
------------------------------------------------------------------------------------------------------
-- 7. Вставка данных в таблицу с информацией о платежах
	create table dds.payments_e_krylova (
		payment_id int not null primary key,
		client_id int not null,
		payment_date timestamp,
		currency int,
		amount numeric(15,2),
		payment_method int,
		FOREIGN KEY (client_id) REFERENCES dds.clients_e_krylova(client_id) ON DELETE CASCADE,
		FOREIGN KEY (currency) REFERENCES dds.currency_e_krylova(currency_id) ON DELETE CASCADE,
		FOREIGN KEY (payment_method) REFERENCES dds.payment_methods_e_krylova(payment_method_id) ON DELETE CASCADE
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
		where payment_id is not null and payment_date between '2010-01-01' and '2026-01-01';
------------------------------------------------------------------------------------------------------
-- 8. Вставка данных в таблицу с информацией о методах платежей
create table dds.transaction_types_e_krylova (
		transaction_type_id SERIAL primary key,
		transaction_type_name varchar(50) not null
	)
	distributed by (transaction_type_id);

    -- Вставка данных в таблицу 
   insert into dds.transaction_types_e_krylova (transaction_type_name)
		select 
			transaction_type
		from ods.transactions_e_krylova
		WHERE LOWER(transaction_type) NOT IN ('\nan\', 'transaction_type')
		group by transaction_type;
------------------------------------------------------------------------------------------------------
-- 9. Вставка данных в таблицу с информацией о транзакциях
	create table dds.transactions_e_krylova (
		transaction_id int not null primary key,
		client_id int not null,
		transaction_date TIMESTAMPTZ,
		transaction_type int,
		account_number varchar(100),
		currency int,
		amount numeric(15,2),
		FOREIGN KEY (client_id) REFERENCES dds.clients_e_krylova(client_id) ON DELETE CASCADE,
		FOREIGN KEY (currency) REFERENCES dds.currency_e_krylova(currency_id) ON DELETE CASCADE,
		FOREIGN KEY (transaction_type) REFERENCES dds.transaction_types_e_krylova(transaction_type_id) ON DELETE CASCADE
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
		where transaction_id is not null and transaction_date between '2010-01-01' and '2026-01-01';

END;
$procedure$
;
