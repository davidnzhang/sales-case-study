with source as (
    select * from {{ ref('BundledProducts') }}
),

renamed as (
    select
        "BundledProductID"::int as bundled_product_id,
        "BundledProductName"::varchar as bundled_product_name
    from source
)

select distinct on (bundled_product_id) * from renamed