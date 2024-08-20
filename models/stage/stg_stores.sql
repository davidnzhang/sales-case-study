with source as (
    select * from {{ ref('Stores') }}
),

renamed as (
    select
        "StoreID"::int as store_id,
        "StoreName"::varchar as store_name,
        "Location"::varchar as store_city,
        "Manager"::varchar as store_manager_first_name
    from source
)

select distinct on (store_id) * from renamed