with source as (
    select * from {{ ref('Promotions') }}
),

renamed as (
    select
        "PromotionID"::int as promotion_id,
        "PromotionName"::varchar as promotion_name,
        "StartDate"::date as promotion_start_date,
        "EndDate"::date as promotion_end_date,
        "DiscountPercentage"::int as discount_percentage
    from source
)

select distinct on (promotion_id) * from renamed