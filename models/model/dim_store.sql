with staged_stores as (
    select * from {{ ref('stg_stores') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,
        store_name,
        store_city,
        store_manager_first_name
    from staged_stores

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['null']) }} as store_key,
        'Unknown' as store_name,
        'Unknown' as store_city,
        'Unknown' as store_manager_first_name
)

select * from final