{{ config(materialized='view', schema='GOLD') }}

SELECT 
    
FROM {{ref('transform')}}

