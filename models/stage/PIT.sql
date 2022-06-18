WITH pre AS (
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_PK ORDER BY effective_from DESC) AS rn
FROM {{ ref('sat_customer_details') }}
WHERE 1=1
	AND effective_from < '2022-06-18 21:00:00'
	-- AND last_name = 'Trumpino'
)

SELECT
    *
FROM pre
WHERE rn = 1