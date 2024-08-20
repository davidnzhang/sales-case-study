with staged_bundled_products as (
    select * from {{ ref('stg_bundled_products') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['bundled_product_id']) }} as bundled_product_key,

        -- attributes
        bundled_product_name
    from staged_bundled_products

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['null']) }} as bundled_product_key,
        'Unknown' as bundled_product_name
)

select * from final