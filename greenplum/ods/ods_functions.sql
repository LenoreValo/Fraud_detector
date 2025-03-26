-- Реализация функций для преобразования типов

-- Преобразование строки в TIMESTAMPTZ
CREATE OR REPLACE FUNCTION ods.safe_to_timestamptz_with_check(input_value TEXT) 
RETURNS TIMESTAMPTZ AS $$
BEGIN
    -- Проверяем, соответствует ли строка формату ISO 8601
    IF input_value ~ '^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?)$' THEN
        RETURN input_value::TIMESTAMPTZ;
    ELSE
        RETURN NULL;  -- Возвращаем NULL для некорректных форматов
    END IF;
EXCEPTION
    -- Если возникла ошибка при преобразовании, возвращаем NULL
    WHEN others THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------------------------------
-- Преобразование строки в FLOAT4
CREATE OR REPLACE FUNCTION ods.safe_to_float_with_check(input_value TEXT) 
RETURNS FLOAT4 AS $$
BEGIN
    -- Проверяем, соответствует ли строка формату
    IF input_value ~ '^-?\d+(\.\d+)?$' THEN
        RETURN input_value::FLOAT4;
    ELSE
        RETURN NULL;  -- Возвращаем NULL для некорректных форматов
    END IF;
EXCEPTION
    -- Если возникла ошибка при преобразовании, возвращаем NULL
    WHEN others THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------
-- Преобразование строки в INET (для IP-адресов)
CREATE OR REPLACE FUNCTION ods.safe_to_inet_with_check(input_value TEXT) 
RETURNS INET AS $$
BEGIN
    -- Проверяем, соответствует ли строка формату 
    IF input_value ~ '^(\d{1,3}\.){3}\d{1,3}$' THEN
        RETURN input_value::INET;
    ELSE
        RETURN NULL;  -- Возвращаем NULL для некорректных форматов
    END IF;
EXCEPTION
    -- Если возникла ошибка при преобразовании, возвращаем NULL
    WHEN others THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------
-- Преобразование строки в DATE
CREATE OR REPLACE FUNCTION ods.safe_to_date_with_check(input_value TEXT) 
RETURNS DATE AS $$
BEGIN
    -- Проверяем, соответствует ли строка формату ISO
    IF input_value ~ '^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?)$' THEN
        RETURN input_value::DATE;
    ELSE
        RETURN NULL;  -- Возвращаем NULL для некорректных форматов
    END IF;
EXCEPTION
    -- Если возникла ошибка при преобразовании, возвращаем NULL
    WHEN others THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------
-- Преобразование строки в NUMERIC(15,2)
CREATE OR REPLACE FUNCTION ods.safe_to_numeric_with_check(input_value TEXT) 
RETURNS NUMERIC(15,2) AS $$
BEGIN
    -- Проверяем, соответствует ли строка формату ISO
    IF input_value ~ '^\s*-?\d+(\.\d{1,2})?\s*$' THEN
        RETURN input_value::NUMERIC(15,2);
    ELSE
        RETURN NULL;  -- Возвращаем NULL для некорректных форматов
    END IF;
EXCEPTION
    -- Если возникла ошибка при преобразовании, возвращаем NULL
    WHEN others THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------



