with staged_customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,

        -- attributes
        customer_name,
        customer_age,
        customer_gender,
        customer_city
    from staged_customers

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['null']) }} as customer_key,
        'Unknown' as customer_name,
        null as customer_age,
        'Unknown' as customer_gender,
        'Unknown' as customer_city
)

select * from final