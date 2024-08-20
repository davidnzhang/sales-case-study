with source as (
    select * from {{ ref('Products') }}
),

renamed as (
    select
        "ProductID"::int as product_id,
        "CategoryID"::int as category_id,
        "ProductName"::varchar as product_name,
        "Price"::numeric(16,2) as product_price_dollars
    from source
)

select distinct on (product_id) * from renamed