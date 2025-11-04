with 
user_orders as (  
  select 
    user_id,
    date(created_at) as order_date,
    date_trunc(
      min(date(created_at)) over (partition by user_id), 
    month) as cohort_month,
    sale_price
  from `bigquery-public-data.thelook_ecommerce.order_items` 
  where status not in ('Cancelled', 'Returned')
  ),

cohort_desc as (
  select
    cohort_month, 
    max(order_date)                         as last_purchase_month,
    count(distinct user_id)                 as cohort_size
  from user_orders
  group by 1
  ),

cohort_calendar as (
  select
    f1.cohort_month,
    f1.cohort_size,
    m + 1 as month_index,
    date_add(
      f1.cohort_month, interval m month
      ) as purchase_month
  from cohort_desc f1
  cross join unnest(
    generate_array(0, date_diff(f1.last_purchase_month, f1.cohort_month, month))) as m
  ),

cohort_arpu as (
  select 
    f1.cohort_month                           as cohort_month,
    date_trunc(order_date, month)             as purchase_month,
    cohort_size, 
    sum(sale_price)                           as total_revenue,
    safe_divide(sum(sale_price), cohort_size) as arpu
  from user_orders as f1
  left outer join
    cohort_desc as f2 on f1.cohort_month = f2.cohort_month
  group by 1, 2, 3
  order by 1, 2)

select 
  f1.cohort_month,
  f1.cohort_size,
  f1.purchase_month,
  f1.month_index,
  coalesce(total_revenue, 0) as total_revenue,
  coalesce(arpu, 0)          as arpu,
  sum(arpu) over (partition by f1.cohort_month order by f1.purchase_month) as cumulative_ltv
from cohort_calendar as f1
left outer join cohort_arpu as f2 on f1.purchase_month = f2.purchase_month and f1.cohort_month = f2.cohort_month
order by 1, 2

