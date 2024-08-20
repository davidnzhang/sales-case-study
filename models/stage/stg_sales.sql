with source as (
    select * from {{ ref('Sales') }}
),

renamed as (
    select
        "OrderID"::int as order_id,
        "CustomerID"::int as customer_id,
        "StoreID"::int as store_id,
        "BundledProductID"::int as bundled_product_id,
        to_date("OrderDate", 'DD/MM/YYYY') as order_date,
        "SalesAmount"::numeric(16,2) as sales_amount_dollars
    from source
)

select distinct on (order_id) * from renamed
where order_id is not null -- remove null rows