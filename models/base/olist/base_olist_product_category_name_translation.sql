with source as (

    select *
    from {{ source('olist', 'product_category_name_translation') }}

)

select *
from source
