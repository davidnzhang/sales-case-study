with source as (
    select * from {{ ref('Categories') }}
),

renamed as (
    select
        "CategoryID"::int as category_id,
        "CategoryName"::varchar as category_name
    from source
)

select distinct on (category_id) * from renamed