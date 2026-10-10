{{ config(materialized='view', schema='GOLD') }}

SELECT 
    DRS_REVIEW_ID,
    DRS_REVIEW_ID,
    DRS_ORIGINAL_DECISION,
    DRS_OUTCOME
FROM {{ref('transform')}}

