with source as (
    select * from {{ ref('BundledProductsPromotions') }}
),

renamed as (
    select
        "BundledProductID"::int as bundled_product_id,
        "PromotionID"::int as promotion_id
    from source
)

select * from renamed