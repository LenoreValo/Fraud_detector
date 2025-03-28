-- Реализация функций для преобразования типов

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