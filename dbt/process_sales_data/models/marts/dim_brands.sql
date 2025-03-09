{{
    config(
        materialized='incremental',
        unique_key='brand_id'
    )
}}

WITH stg_sales_data AS (
    SELECT 
        DISTINCT brand,
        -- TODO: could be changed to reflect the first sale_date instead of the current_date
        current_date AS start_date  -- Set current date as start_date for new records
    FROM {{ ref('stg_sales_data') }}
    {% if is_incremental() %}
    -- Only get records from `stg_sales_data` from the last sale_date ingested 
    WHERE sale_date = (SELECT MAX(sale_date) FROM {{ ref('stg_sales_data') }})
    {% endif %}
),
new_brands AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY brand) AS brand_id, -- surrogate key
        brand AS brand_name,
        current_date AS start_date,
        NULL::DATE AS end_date, 
        TRUE AS is_current
    FROM stg_sales_data
    {% if is_incremental() %}
    WHERE brand NOT IN (SELECT brand FROM {{ this }})
    {% endif %}
)
SELECT * FROM new_brands