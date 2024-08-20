with staged_promotions as (
    select * from {{ ref('stg_promotions') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['promotion_id']) }} as promotion_key,

        -- promotion attributes
        promotion_name,
        promotion_start_date,
        promotion_end_date,
        discount_percentage
    from staged_promotions

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['null']) }} as promotion_key,
        'No Promotion' as promotion_name,
        null as promotion_start_date,
        null as promotion_end_date,
        0 as discount_percentage
)

select * from final