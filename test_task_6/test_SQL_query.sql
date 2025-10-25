 -- �������� 1: ��������� ����������� ������ 
 /*
�������������� ����� �������, �������� �����, ���� ��� ������� ���������� �������� 
����� ����������, �볺���, �������� ���� ���������� �� ������� ���� ����� ���������� 
�� ����� ����� ��� ���������*/

select
	  f1.[id] as [order_id]
	, [name]
	, [total_amount]
	, rank() over (order by [total_amount] desc) as [rank]
from
	[ShopDB].[dbo].[orders] as f1
inner join 
	[ShopDB].[dbo].[customers] as f3 on f1.customer_id = f3.id


--�������� 2: ���������� ��������� �� ����������� ����� 
/*�����, �� �� ������ ������� ��������� �� ��������� ������ �� ���������. �������� 
�����, ���� ������� ���-10 ������ �� ��������� �� ����� (profit = ������� ��������� * 
���� �� �������). ���������� ����� ������ ������� �������� ������ �����*/ 
declare @start_date date, @end_date date

set @start_date = '2023-04-01'
set @end_date = '2023-05-01'

select top 10 with ties 
	  [product_name]
	, sum([quantity] * [price]) as [profit]
from 
	[ShopDB].[dbo].[order_items] as f1 
inner join 
	[ShopDB].[dbo].[orders] as f2 on f1.[order_id]= f2.[id] 
where f2.[order_date] >= @start_date
  and f2.[order_date] <  @end_date
group by [product_name]
order by [profit] desc

--�������� 3: ��������� �� ��������� ����������� ������ 
/*��� ��������� �������� ������� ��������� ���������� ����� ��� ����������.  
�������� SQL-������, ����: 
1. �������� ���� ������� ��� ��������� ���������� ����� ��� ����������. 
2. �������� ������, ���� ��� ��� ��� �������� ���������� ����������� 
���������� ���������� ��� ����� �������� � ������� �����.*/

create table orders_history (
    id int NOT NULL,
    customer_id int NOT NULL,
    order_date date NOT NULL,
    total_amount decimal(10,2) NOT NULL,
	change_date datetime  NOT NULL
);
go 

create trigger trg_orders_history
on orders
after update, delete
as 
begin
    set nocount on;

    insert into orders_history (id, customer_id, order_date, total_amount, change_date)
    select 
		f1.id
	  , f1.customer_id
	  , f1.order_date
	  , f1.total_amount
	  , getdate() as change_date 
    from 
		deleted as f1;
end;
go

--�������� 4: ��������� ��������� 
/*�������� ���������, �� ����������, ��� ������ ������������� �볺��� � ������� 
������ ��� ���� ���������, ��������� �������� ���� ������� ����������, � ����� 
������� ��������� �������� � ������� ���������*/

create procedure get_customer_orders
    @customer_id int

as
begin

select 
     f1.[id] as [order_id]
   , [order_date]
   , [total_amount]
   , count(distinct [product_name]) as unique_product_count
from 
	[ShopDB].[dbo].[orders] as f1 
inner join 
	[ShopDB].[dbo].[order_items] as f2 on f1.[id] = f2.[order_id]
where [customer_id] = @customer_id
group by f1.[id], [order_date], [total_amount]

end;
go

exec get_customer_orders @customer_id = 2

--�������� 5: ������� �������� ����� 
/*�����, �� � ��� ����� �'������� �������, ����� ��� � ��������� ������� �������� ����. 
�������� SQL-����� ��� ������ �� ����������� ��� ����� ���������, ��� ����� 
���������� ��� ����� �����, ��� ���� ��������� �������� ����� ������ � ���������*/

update f1
set 
	f1.[total_amount] = f2.amount_new 
from
	[ShopDB].[dbo].[orders] as f1
left outer join
	(select 
		 [order_id] 
	   , sum([quantity] * [price]) as amount_new
	from [ShopDB].[dbo].[order_items] as f2 
	group by [order_id]) as f2 
on f1.[id] = f2.[order_id]
where [total_amount] < 0


--�������� 6: ������������ ����� 
/*������������ ���� ��������� ���� ����� ��� ���������� ������������� �� 
�������������� � ������ �������� ��������� ������� ��������� �� �볺��� (�� ����� 
��������� 2���. ��������� �� ����). � ����� ������������ ���������� ����� ��� 
������������ �������� ���*/

-- 1. � ������� [dbo].[products] �� �������� �������, �������� ��� ��� ������ �������,
--    � ������� products ��������������� �� ������� ������, ������ �� �� ���������� ��� ����� ������, ������������� � �.�

-- 2. � ������� [dbo].[order_items] ��������������� �� [product_name] � [product_id], �� ������� ���� �� products.id
--    �� ���� ����������� ��'������� �������, �� ��������� ������ �������� � ���������� ������� ������, �������� � �.�

-- 3. ������ ���������� �� ������� [dbo].[orders] �� [dbo].[order_items], ��� ����������� ������ � ���������, 
--    ��������� �� ������� [dbo].[orders].[order_date] ��� ������ �������� ���������� �� ��������� ��� (�������, ��������)
--    �� [dbo].[order_items].[order_id] ��� ������ ������ ������ �� orders.
--    ��������� ����� [dbo].[orders].[customer_id] - ���������� �볺���,
--    [dbo].[order_items].[product_id] - ����� �� �������.

-- 4. ����� ������ �� ������� [dbo].[orders] �� [dbo].[order_items] ������� [last_updated_at], ��������� ������� �������� �����

-- 5. ��� orders, order_items, products �������� ������� *_history ��� ���������

-- 6. ����� ����� ���������� ������� � ���������� ��� ��������, ��� ������� ���� �� ����� �� ��� ��� �� �����������,
--    � ��������� �������� ����� ���� ��� ��������� ��� �� ��������� ���������

-- 7. � ����� ����� �������� ��� ����������� ������ �����

-- 8. � � �� ������ ������� � ������ ��� �� ����, ��� ������ �������, � ����� ������� � ��������� �� ����


--�������� 7: ������ � ����������� ������ 
/*�������� SQL ����� ��� ���������� ���������� �������� ����� �볺��� �� ������� ��. 
��� ����, �������� ������� ������� �������� �볺��� �� �������.*/ 

with cte as (
	select 
		 format([registration_date], 'yyyy-MM') as year_month
	   , cast(count([id]) as float)             as new_customers_count
	from
		[ShopDB].[dbo].[customers]
	group by format([registration_date], 'yyyy-MM'))
	
select 
	 year_month
   , lag(new_customers_count) over (order by year_month)                               as prev_customers_count
   , new_customers_count
   , new_customers_count - lag(new_customers_count, 1, 0) over (order by year_month)   as abs_growth
   , (new_customers_count - lag(new_customers_count, 1, 0) over (order by year_month)) 
      / lag(new_customers_count) over (order by year_month) * 100                      as perc_growth
into #temp_table
from cte

select 
     * 
from #temp_table

select 
     avg(perc_growth) as avg_perc_growth 
from #temp_table