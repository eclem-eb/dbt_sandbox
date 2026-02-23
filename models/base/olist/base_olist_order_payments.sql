with source as (

    select *
    from {{ source('olist', 'order_payments') }}

)

select *
from source
