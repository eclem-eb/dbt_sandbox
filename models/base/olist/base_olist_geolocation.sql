with source as (

    select *
    from {{ source('olist', 'geolocation') }}

)

select *
from source
