{{ config(materialized='view', schema='GOLD') }}

SELECT 
    TEAM_ID,
    TEAM_NAME
FROM {{ref('transform')}}

