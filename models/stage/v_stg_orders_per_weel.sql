WITH order_detals AS
(
SELECT order_date, status, COUNT(order_date) AS amount
FROM {{ ref('v_stg_orders') }}
GROUP BY order_date, status
),

calendare AS (

    SELECT
    date,
    number_day,
    number_week

    FROM "postgres"."dbt"."source_cal_2018"
)

SELECT DISTINCT MIN(order_date) OVER (PARTITION BY number_week ORDER BY number_week) AS order_week,
od.status, COUNT(order_date) OVER (PARTITION BY number_week ORDER BY number_week) AS amount
FROM order_detals AS od
LEFT JOIN calendare AS c ON c.date = od.order_date