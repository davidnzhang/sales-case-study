with staged_sales as (
    select * from {{ ref('stg_sales') }}
),

staged_promotions as (
    select * from {{ ref('stg_promotions') }}
),

staged_bundled_products_promotions as (
    select * from {{ ref('stg_bundled_products_promotions') }}
),

final as (
    select
        staged_sales.order_id,
        {{ dbt_utils.generate_surrogate_key(['staged_sales.customer_id']) }} as customer_key,
        {{ dbt_utils.generate_surrogate_key(['staged_sales.store_id']) }} as store_key,
        {{ dbt_utils.generate_surrogate_key(['staged_sales.bundled_product_id']) }} as bundled_product_key,
        -- staged_sales.bundled_product_id,
        case
            when staged_bundled_products_promotions.bundled_product_id = staged_sales.bundled_product_id
                then {{ dbt_utils.generate_surrogate_key(['staged_promotions.promotion_id']) }}
            else {{ dbt_utils.generate_surrogate_key(['null']) }}
        end as promotion_key,
        -- case
        --     when staged_bundled_products_promotions.bundled_product_id = staged_sales.bundled_product_id
        --         then staged_promotions.promotion_id
        --     else null
        -- end as promotion_key_2,
        staged_sales.order_date,
        staged_sales.sales_amount_dollars
    from staged_sales
    left join staged_promotions
        on staged_sales.order_date between staged_promotions.promotion_start_date and staged_promotions.promotion_end_date
    left join staged_bundled_products_promotions
        on staged_promotions.promotion_id = staged_bundled_products_promotions.promotion_id
)

select * from final