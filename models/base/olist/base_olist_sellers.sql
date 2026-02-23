with source as (

    select *
    from {{ source('olist', 'sellers') }}

)

select *
from source
