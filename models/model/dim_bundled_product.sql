with staged_bundled_products as (
    select * from {{ ref('stg_bundled_products') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['bundled_product_id']) }} as bundled_product_key,

        -- natural key
        bundled_product_id,

        -- attributes
        bundled_product_name
    from staged_bundled_products

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['null']) }} as bundled_product_key,
        -1 as bundled_product_id,
        'Unknown' as bundled_product_name
)

select * from final