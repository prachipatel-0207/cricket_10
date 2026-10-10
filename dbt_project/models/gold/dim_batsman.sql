{{ config(materialized='view', schema='GOLD') }}

SELECT 
    BATSMAN_ID,
    BATSMAN_NAME,
    BATSMAN_DOB,
    BATSMAN_EMAIL,
    BATSMAN_PHONE,
    BATTING_TEAM_ID,
    BATSMAN_NATIONALITY
FROM {{ref('transform')}}  

