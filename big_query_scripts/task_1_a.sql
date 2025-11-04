with cte as (
  select
    user_id,
    order_id,
    created_at, 
    row_number() over (partition by user_id order by created_at) as row_numb
  from `bigquery-public-data.thelook_ecommerce.orders` 
  where status not in ('Cancelled', 'Returned'))

select
  date(created_at)      as order_date,
  count(*)              as orders_count,
  countif(row_numb = 1) as first_orders_count,
  countif(row_numb > 1) as repeat_orders_count,
  safe_divide(countif(row_numb = 1), count(*)) as first_orders_share,
  safe_divide(countif(row_numb > 1), count(*)) as repeat_orders_share
from cte
group by date(created_at)