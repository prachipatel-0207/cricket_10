{{ config(materialized='view', schema='GOLD') }}

SELECT 
    VENUE_ID,
    VENUE_NAME,
    VENUE_CITY,
    VENUE_COUNTRY
FROM {{ref('transform')}}

