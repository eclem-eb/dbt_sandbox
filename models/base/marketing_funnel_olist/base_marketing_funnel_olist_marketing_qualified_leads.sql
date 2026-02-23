with source as (

    select *
    from {{ source('marketing_funnel_olist', 'marketing_qualified_leads') }}

)

select *
from source
