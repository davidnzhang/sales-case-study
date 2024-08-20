with source as (
    select * from {{ ref('BundledProducts_Products') }}
),

renamed as (
    select
        "BundledProductID"::int as bundled_product_id,
        "ProductID"::int as product_id
    from source
)

select * from renamed