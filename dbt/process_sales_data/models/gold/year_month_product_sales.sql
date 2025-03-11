{{
    config(
        materialized='incremental',
        keys=['month', 'product_name', 'retailer_name', 'channel'],
        indexes=[
            {'columns': ['month'], 'type': 'btree'},
            {'columns': ['product_name'], 'type': 'btree'},
            {'columns': ['retailer_name'], 'type': 'btree'},
            {'columns': ['channel'], 'type': 'btree'},
        ]
    )
}}

SELECT
    TO_CHAR(DATE_TRUNC('month', s.sale_date), 'YYYY')::int AS year,
    TO_CHAR(DATE_TRUNC('month', s.sale_date), 'MM')::int AS month,
    p.product_name,
    r.retailer_name,
    r.channel,
    SUM(s.quantity) AS total_quantity,
    SUM(s.total) AS total_revenue
FROM {{ ref('fact_sales') }} AS s
INNER JOIN {{ ref('dim_products') }} AS p
    ON s.product_id = p.product_id
INNER JOIN {{ ref('dim_retailers') }} AS r
    ON s.retailer_id = r.retailer_id
WHERE s.quantity > 0
AND s.total > 0
{% if is_incremental() %}
  AND s.sale_date >= (SELECT DATE_TRUNC('month', MAX(sale_date)) FROM {{ ref('stg_sales_data') }})
{% endif %}
GROUP BY year, month, product_name, retailer_name, channel