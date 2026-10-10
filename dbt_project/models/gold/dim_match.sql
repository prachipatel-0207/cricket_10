{{ config(materialized='view', schema='GOLD') }}

SELECT 
    MATCH_ID,
    MATCH_DATE,
    MATCH_FORMAT,
    MATCH_STAGE,
    TOURNMENT_NAME,
    SEASON,
    TOSS_DECISION 
FROM {{ref('transform')}}

