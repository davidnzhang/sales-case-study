with source as (
    select * from {{ ref('Customers') }}
),

renamed as (
    select
        "CustomerID"::int as customer_id,
        coalesce("Name", 'Unknown')::varchar as customer_name,
        "Age"::int as customer_age,
        coalesce("Gender", 'Unknown')::varchar as customer_gender, -- example null handling, might be more prudent to try to understand cause of null and handle them downstream
        coalesce("City", 'Unknown')::varchar as customer_city
    from source
)

select distinct on (customer_id) * from renamed