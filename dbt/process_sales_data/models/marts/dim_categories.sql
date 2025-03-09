{{
    config(
        materialized='incremental',
        unique_key='category_id'
    )
}}

WITH stg_sales_data AS (
    SELECT 
        DISTINCT category,
        -- TODO: could be changed to reflect the first sale_date instead of the current_date
        current_date AS start_date 
    FROM {{ ref('stg_sales_data') }}
    {% if is_incremental() %}
    -- Only get records from `stg_sales_data` from the last sale_date ingested 
    WHERE sale_date = (SELECT MAX(sale_date) FROM {{ ref('stg_sales_data') }})
    {% endif %}
),
new_categories AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY category) AS category_id,  -- surrogate key
        category AS category_name,
        current_date AS start_date,
        NULL::DATE AS end_date,  -- null expiration so it's the current version 
        TRUE AS is_current -- flag as current for easy join to fact sales
    FROM stg_sales_data
    {% if is_incremental() %}
    WHERE category NOT IN (SELECT category_name FROM {{ this }})
    {% endif %}
)
SELECT * FROM new_categories