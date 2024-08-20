with staged_bundled_products_products as (
    select * from {{ ref('stg_bundled_products_products') }}
),

final as (
    select
        -- surrogate keys
        {{ dbt_utils.generate_surrogate_key(['bundled_product_id']) }} as bundled_product_key,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

        -- natural keys
        bundled_product_id,
        product_id
    from staged_bundled_products_products
)

select * from final