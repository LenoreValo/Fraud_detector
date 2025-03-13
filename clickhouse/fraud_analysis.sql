-------------------------------------------------------------------------------------------------------------------------
-- Выявление клиентов с аномально большим кол-вом транзакций в один день и аномально большими суммами транзакций 
-------------------------------------------------------------------------------------------------------------------------

WITH daily_aggregated_data AS (
    SELECT
        client_id,
        toDate(transaction_date) AS transaction_day,
        COUNT(transaction_id) AS daily_transaction_count,
        SUM(total_amount) AS daily_total_amount
    FROM wave18_team_b.gp_transactions_e_krylova
    GROUP BY client_id, transaction_day
),
quantiles AS (
    SELECT
        quantile(0.25)(daily_transaction_count) AS q1_daily_transaction_count,
        quantile(0.75)(daily_transaction_count) AS q3_daily_transaction_count,
        quantile(0.25)(daily_total_amount) AS q1_daily_total_amount,
        quantile(0.75)(daily_total_amount) AS q3_daily_total_amount
    FROM daily_aggregated_data
),
iqr AS (
    SELECT
        q3_daily_transaction_count - q1_daily_transaction_count AS iqr_daily_transaction_count,
        q3_daily_total_amount - q1_daily_total_amount AS iqr_daily_total_amount,
        q1_daily_transaction_count,
        q3_daily_transaction_count,
        q1_daily_total_amount,
        q3_daily_total_amount
    FROM quantiles
)
SELECT
    a.client_id,
    a.transaction_day,
    a.daily_transaction_count,
    a.daily_total_amount
FROM daily_aggregated_data a, iqr i
WHERE
    a.daily_transaction_count < (i.q1_daily_transaction_count - 1.5 * i.iqr_daily_transaction_count)
    OR a.daily_transaction_count > (i.q3_daily_transaction_count + 1.5 * i.iqr_daily_transaction_count)
    OR a.daily_total_amount < (i.q1_daily_total_amount - 1.5 * i.iqr_daily_total_amount)
    OR a.daily_total_amount > (i.q3_daily_total_amount + 1.5 * i.iqr_daily_total_amount)
    ;