-- Реализация функций для преобразования типов

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