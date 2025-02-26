{{
    config(
        materialized='table'
    )
}}

with year_adn_quarter as (
    SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, 
EXTRACT(QUARTER FROM pickup_datetime) AS quarter, service_type, total_amount
    from {{ ref('fact_trips') }}
),
agg as (
    select service_type, year, quarter, sum(total_amount) as total_amount from year_adn_quarter
group by service_type, year, quarter
)
SELECT 
    service_type,
    year,
    quarter,
    total_amount,
    LAG(total_amount) OVER (PARTITION BY service_type, quarter ORDER BY year) AS prev_year_total_amount,
    CASE 
        WHEN LAG(total_amount) OVER (PARTITION BY service_type, quarter ORDER BY year) = 0 THEN NULL  -- prevent division by zero
        ELSE ROUND((total_amount - LAG(total_amount) OVER (PARTITION BY service_type, quarter ORDER BY year)) / NULLIF(LAG(total_amount) OVER (PARTITION BY service_type, quarter ORDER BY year
            ), 0) * 100, 2
        )
    END AS yoy_percentage_change
FROM agg
ORDER BY service_type, year, quarter