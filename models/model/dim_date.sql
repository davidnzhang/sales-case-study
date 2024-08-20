with recursive date_spine as (
    select 
        date '2024-01-01' as calendar_date
    union all
    select 
        calendar_date + int '1'
    from date_spine
    where calendar_date < '2026-12-31'
),

dates as (
    select
        -- date key
        calendar_date as date,

        -- date attributes
        extract(isodow from calendar_date) as day_of_week,
        case
            when extract(isodow from calendar_date) in (6, 7) then true
            else false
        end as is_weekend,
        to_char(calendar_date, 'Dy')::text as day_of_week_name_short,
        to_char(calendar_date, 'Day')::text as day_of_week_name,
        extract(day from calendar_date)::int as day_of_month,
        extract(doy from calendar_date)::int as day_of_year,
        to_char(calendar_date, 'w')::int as week_of_month,
        extract(week from calendar_date)::int as week_of_year,
        extract(month from calendar_date)::int as month_of_year,
        to_char(calendar_date, 'Mon')::text as month_name_short,
        to_char(calendar_date, 'Month')::text as month_name,
        extract(quarter from calendar_date)::int as quarter_of_year,
        mod(extract(quarter from calendar_date)::int+1, 4)+1 as quarter_of_financial_year,
        calendar_date - date_trunc('quarter', calendar_date)::date + 1 as day_of_quarter,
        extract(year from calendar_date)::int as year,
        extract(year from (calendar_date + interval '6 month'))::int as financial_year,
        calendar_date + (1 - extract(isodow from calendar_date))::int as week_start_date,
        calendar_date + (7 - extract(isodow from calendar_date))::int as week_end_date,
        calendar_date + (1 - extract(day from calendar_date))::int as month_start_date,
        (date_trunc('month', calendar_date) + interval '1 month - 1 day')::date as month_end_date,
        date_trunc('quarter', calendar_date)::date as quarter_start_date,
        (date_trunc('quarter', calendar_date) + interval '3 month - 1 day')::date as quarter_end_date
    from date_spine
)

select * from dates