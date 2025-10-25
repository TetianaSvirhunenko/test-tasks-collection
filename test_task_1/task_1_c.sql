with orders as (
  select
    user_id,
    min(date(created_at)) as first_order_date
  from `bigquery-public-data.thelook_ecommerce.orders` 
  where status not in ('Cancelled', 'Returned')
  group by 1
),

users as (
  select 
    id,
    traffic_source,
    date(created_at) as created_date
  from `bigquery-public-data.thelook_ecommerce.users`
)

select 
  traffic_source,
  date_trunc(created_date, month)                     as cohort_month,
  count(distinct f1.id)                               as sample_size,
  avg(date_diff(first_order_date, created_date, day)) as days_to_first_purchase
from users as f1
inner join orders as f2 on f1.id = f2.user_id
group by 1, 2
order by 2, 1
