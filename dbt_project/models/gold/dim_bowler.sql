{{ config(materialized='view', schema='GOLD') }}

SELECT 
    BOWLER_ID,
    BOWLER_NAME,
    BOWLER_STYLE,
    BOWLER_NATIONALITY,
    
FROM {{ref('transform')}}  

