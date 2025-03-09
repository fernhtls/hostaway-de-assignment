{{
    config(
        materialized='incremental',
        unique_key='retailer_id'
    )
}}

WITH stg_sales_data AS (
    SELECT 
        DISTINCT
        retailer_id,
        retailer_name,
        -- TODO: both properties here could be dims as well
        -- but regarding the sample and example data, this is not needed
        CASE channel WHEN 'None' THEN NULL ELSE channel END AS channel,
        CASE location WHEN 'None' THEN NULL ELSE location END AS location,
        -- TODO: could be changed to reflect the first sale_date instead of the current_date
        current_date AS start_date 
    FROM {{ ref('stg_sales_data') }}
    {% if is_incremental() %}
    -- Only get records from `stg_sales_data` from the last sale_date ingested 
    WHERE sale_date = (SELECT MAX(sale_date) FROM {{ ref('stg_sales_data') }})
    {% endif %}
),
new_retailers AS (
    SELECT
        retailer_id,
        retailer_name,
        channel,
        location,
        NULL::DATE AS end_date,  -- null expiration so it's the current version 
        TRUE AS is_current -- flag as current for easy join to fact sales
    FROM stg_sales_data
    {% if is_incremental() %}
    WHERE retailer_id NOT IN (SELECT retailer_id FROM {{ this }})
    {% endif %}
)
SELECT * FROM new_retailers