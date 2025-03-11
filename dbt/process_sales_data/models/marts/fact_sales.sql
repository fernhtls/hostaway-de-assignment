{{
    config(
        materialized='incremental',
        unique_key='sale_id',
        indexes=[
            {'columns': ['product_id'], 'type': 'hash'},
            {'columns': ['retailer_id'], 'type': 'hash'},
            {'columns': ['sale_date'], 'type': 'btree'},
        ]
    )
}}

WITH stg_sales_data AS (
    SELECT 
        DISTINCT
        sale_id,
        sale_date,
        product_id,
        retailer_id,
        price,
        quantity,
        total
    FROM {{ ref('stg_sales_data') }}
    {% if is_incremental() %}
    -- Only get records from `stg_sales_data` from the last sale_date ingested 
    WHERE sale_date = (SELECT MAX(sale_date) FROM {{ ref('stg_sales_data') }})
    {% endif %}
),
new_sales AS (
    SELECT
        *
    FROM stg_sales_data
)
SELECT * FROM new_sales