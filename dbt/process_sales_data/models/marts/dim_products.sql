{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

WITH stg_sales_data AS (
    SELECT 
        DISTINCT
        product_id,
        product_name,
        category,
        brand,
        -- TODO: could be changed to reflect the first sale_date instead of the current_date
        current_date AS start_date 
    FROM {{ ref('stg_sales_data') }}
    {% if is_incremental() %}
    -- Only get records from `stg_sales_data` from the last sale_date ingested 
    WHERE sale_date = (SELECT MAX(sale_date) FROM {{ ref('stg_sales_data') }})
    {% endif %}
),
new_products AS (
    SELECT
        -- no surrogate key for products - might be needed in case of SCD
        product_id,
        product_name,
        dc.category_id,
        db.brand_id,
        category AS category_name,
        current_date AS start_date,
        NULL::DATE AS end_date,  -- null expiration so it's the current version 
        TRUE AS is_current -- flag as current for easy join to fact sales
    FROM stg_sales_data
    LEFT OUTER JOIN {{ ref('dim_categories') }} AS dc
        ON stg_sales_data.category = dc.category_name
    LEFT OUTER JOIN {{ ref('dim_brands') }} AS db
        ON stg_sales_data.brand = db.brand_name
    {% if is_incremental() %}
    WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
    {% endif %}
)
SELECT * FROM new_products