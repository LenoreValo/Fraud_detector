---------------------------------------------------------------------------------------------------------------------
-- Результаты запросов покажут клиентов, которые совершили аномально большое количество транзакций или потратили аномально 
-- большие суммы в конкретный день . Эти клиенты могут быть подозрительными и требовать дополнительной проверки.
---------------------------------------------------------------------------------------------------------------------
WITH daily_aggregated_data AS (
    SELECT
        client_id,
        DATE(transaction_date) AS transaction_day,
        COUNT(transaction_id) AS daily_transaction_count,
        SUM(total_amount) AS daily_total_amount
    FROM dm.transactions_e_krylova
    GROUP BY client_id, transaction_day
),
stats AS (
    SELECT
        AVG(daily_transaction_count) AS avg_daily_transaction_count,
        STDDEV(daily_transaction_count) AS stddev_daily_transaction_count,
        AVG(daily_total_amount) AS avg_daily_total_amount,
        STDDEV(daily_total_amount) AS stddev_daily_total_amount
    FROM daily_aggregated_data
)
SELECT
    a.client_id,
    a.transaction_day,
    a.daily_transaction_count,
    a.daily_total_amount
FROM daily_aggregated_data a, stats s
WHERE
    a.daily_transaction_count > (s.avg_daily_transaction_count + 3 * s.stddev_daily_transaction_count)
    OR a.daily_total_amount > (s.avg_daily_total_amount + 3 * s.stddev_daily_total_amount);