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
