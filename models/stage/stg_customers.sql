with source as (
    select * from {{ ref('Customers') }}
),

renamed as (
    select
        "CustomerID"::int as customer_id,
        coalesce("Name", 'Unknown')::varchar as customer_name,
        "Age"::int as customer_age,
        coalesce("Gender", 'Unknown')::varchar as customer_gender,
        coalesce("City", 'Unknown')::varchar as customer_city
    from source
)

select distinct on (customer_id) * from renamed