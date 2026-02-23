with source as (

    select *
    from {{ source('marketing_funnel_olist', 'closed_deals') }}

)

select *
from source
