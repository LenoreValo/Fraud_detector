-- Реализация функций для преобразования типов
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