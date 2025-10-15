{{ config(
    materialized='incremental',
    unique_key='report_date'
) }}

WITH network_costs AS (
    SELECT
        postcode,
        town,
        network_cost_per_year,
        "network_cost_per_kWh"
    FROM {{ source('raw_data', 'network_costs') }}
),
price_comparison AS (
    SELECT
        report_date,
        postcode,
        location_name,
        "annual_consumption_in_kWh",
        market,
        ranking,
        gross_price_per_month
    FROM {{ source('raw_data', 'price_comparison') }}
),
product_rates AS (
    SELECT
        product_id,
        band,
        unit_type,
        price_per_unit
    FROM {{ source('raw_data', 'product_rates') }}
    WHERE product_id = 224
)

SELECT 
    pc.report_date,
    pc.postcode,
    pc.location_name,
    pc."annual_consumption_in_kWh",
    pc.market,
    pc.ranking,
    (1 + 0.19) * (
        nc.network_cost_per_year +
        nc."network_cost_per_kWh" * pc."annual_consumption_in_kWh" +
        CASE 
            WHEN pr.band = 'STANDING_CHARGE_ELECTRICITY_MONTHLY_SERVICE_FEE' 
            THEN pr.price_per_unit * 12 
            ELSE 0 
        END +
        CASE 
            WHEN pr.band = 'CONSUMPTION_ELECTRICITY_COMMODITY_RATE_PER_KWH' 
            THEN pr.price_per_unit * pc."annual_consumption_in_kWh" 
            ELSE 0
        END
    ) / 12 AS gross_price_month
FROM {{ source('raw_data', 'price_comparison') }} AS pc
JOIN {{ source('raw_data', 'network_costs') }} AS nc 
    ON nc.postcode = pc.postcode
JOIN {{ source('raw_data', 'product_rates') }} AS pr 
    ON pr.product_id = 224
    AND pr.valid_from <= pc.report_date
    AND (pr.valid_to IS NULL OR pr.valid_to >= pc.report_date)
