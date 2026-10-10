{{ config(materialized='view', schema='GOLD') }}

WITH cte1 as (
    select CONTRACT_ID,
    BATSMAN_ID,
    CONTRACT_STATUS,
    ,DELIVERY_ID from {{ref('transform')}}
)

SELECT 
    CONTRACT_ID,
    lag(DELIVERY_TIMESTAMP)over(partition by BATSMAN_ID ORDER BY DELIVERY_TIMESTAMP DESC) AS from_date,
    DELIVERY_TIMESTAMP as to_date,
    CONTACT_GRADE,
    CONTRACT_STATUS,
    case when (
        DELIVERY_TIMESTAMP=max(DELIVERY_TIMESTAMP)over(partition by BATSMAN_ID order by DELIVERY_TIMESTAMP desc) then 
        1 else 0 end
    ) as flag
FROM cte1

