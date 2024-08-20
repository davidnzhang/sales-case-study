with staged_products as (
    select * from {{ ref('stg_products') }}
),

staged_categories as (
    select * from {{ ref('stg_categories') }}
),

flattened_hierarchy as (
    select
        staged_products.product_id,
        staged_products.product_name,
        staged_categories.category_name,
        staged_products.product_price_dollars
    from staged_products
    left join staged_categories
        on staged_products.category_id = staged_categories.category_id
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

        -- product attributes
        product_name,
        category_name,
        product_price_dollars
    from flattened_hierarchy

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['null']) }} as product_key,
        'Unknown' as product_name,
        'Unknown' as category_name,
        null as product_price_dollars
)

select * from final