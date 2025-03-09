{{
    config(
        materialized='incremental', 
        unique_key='sale_id',
    )
}}

SELECT 
    saleid as sale_id,
    date as sale_date,
    productid as product_id,
    productname as product_name,
    brand,
    category,
    retailerid as retailer_id,
    retailername as retailer_name,
    channel,
    location,
    price,
    quantity,
    (price * quantity) as total 
FROM {{ source('raw', 'sales_data_raw') }} 

{% if is_incremental() %}
-- Only insert new or updated rows
WHERE date > (SELECT MAX(sale_date) FROM {{ this }})
{% endif %}
