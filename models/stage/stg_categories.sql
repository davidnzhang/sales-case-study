with source as (
    select * from {{ ref('Categories') }}
),

renamed as (
    select
        "CategoryID"::int as category_id,
        "CategoryName"::varchar as product_category
    from source
)

select distinct on (category_id) * from renamed