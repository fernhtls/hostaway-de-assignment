{% test allowed_channels(model, column_name) %}

WITH validation AS (
    SELECT {{ column_name }} as channel_values 
    FROM {{ model }}
),
validation_errors AS (
   SELECT DISTINCT channel_values
   FROM validation
   WHERE channel_values NOT IN ('Online', 'Offline')
       OR channel_values IS NULL 
)
SELECT * 
FROM validation_errors

{% endtest %}