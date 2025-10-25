 -- Çàâäàííÿ 1: Íàïèñàííÿ àíàë³òè÷íîãî çàïèòó 
 /*
Âèêîğèñòîâóş÷è â³êîíí³ ôóíêö³¿, íàïèø³òü çàïèò, ÿêèé äëÿ êîæíîãî çàìîâëåííÿ âèâîäèòü 
íîìåğ çàìîâëåííÿ, êë³ºíòà, çàãàëüíó ñóìó çàìîâëåííÿ òà ğàíãîâå ì³ñöå öüîãî çàìîâëåííÿ 
çà ñóìîş ñåğåä óñ³õ çàìîâëåíü*/

select
	  f1.[id] as [order_id]
	, [name]
	, [total_amount]
	, rank() over (order by [total_amount] desc) as [rank]
from
	[ShopDB].[dbo].[orders] as f1
inner join 
	[ShopDB].[dbo].[customers] as f3 on f1.customer_id = f3.id


--Çàâäàííÿ 2: Îïòèì³çàö³ÿ çáåğ³ãàííÿ òà àãğåãóâàííÿ äàíèõ 
/*Óÿâ³òü, ùî âè êåğóºòå âåëèêèì ìàãàçèíîì ³ç ì³ëüéîíàìè òîâàğ³â òà çàìîâëåíü. Íàïèø³òü 
çàïèò, ÿêèé ïîâåğòàº òîï-10 òîâàğ³â çà ïğèáóòêîì çà ì³ñÿöü (profit = ê³ëüê³ñòü ïğîäàíîãî * 
ö³íà çà îäèíèöş). Îïòèì³çóéòå çàïèò øâèäêî¿ îáğîáêè âåëèêîãî îáñÿãó äàíèõ*/ 
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

--Çàâäàííÿ 3: Çáåğ³ãàííÿ òà óïğàâë³ííÿ ³ñòîğè÷íèìè äàíèìè 
/*Âàì íåîáõ³äíî ñòâîğèòè ìåõàí³çì çáåğ³ãàííÿ ³ñòîğè÷íèõ äàíèõ ïğî çàìîâëåííÿ.  
Ğîçğîá³òü SQL-ñêğèïò, ÿêèé: 
1. Ñòâîğèòü íîâó òàáëèöş äëÿ çáåğ³ãàííÿ ³ñòîğè÷íèõ äàíèõ ïğî çàìîâëåííÿ. 
2. Íàïèø³òü òğèãåğ, ÿêèé ïğè çì³í³ àáî âèäàëåíí³ çàìîâëåííÿ àâòîìàòè÷íî 
çáåğ³ãàòèìå ³íôîğìàö³ş ïğî ñòàğå çíà÷åííÿ â òàáëèöş ³ñòîğ³¿.*/

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

--Çàâäàííÿ 4: Íàïèñàííÿ ïğîöåäóğè 
/*Íàïèø³òü ïğîöåäóğó, ùî çáåğ³ãàºòüñÿ, ÿêà ïğèéìàº ³äåíòèô³êàòîğ êë³ºíòà ³ ïîâåğòàº 
ñïèñîê óñ³õ éîãî çàìîâëåíü, âêëş÷àş÷è çàãàëüíó ñóìó êîæíîãî çàìîâëåííÿ, à òàêîæ 
ê³ëüê³ñòü óí³êàëüíèõ ïğîäóêò³â ó êîæíîìó çàìîâëåíí³*/

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

--Çàâäàííÿ 5: Îáğîáêà ñêëàäíèõ äàíèõ 
/*Óÿâ³òü, ùî â áàç³ äàíèõ ç'ÿâèëàñÿ ïîìèëêà, ÷åğåç ÿêó â çàìîâëåíí³ âêàçàíî íåãàòèâí³ ñóìè. 
Íàïèø³òü SQL-çàïèò äëÿ ïîøóêó òà âèïğàâëåííÿ âñ³õ òàêèõ çàìîâëåíü, ïğè öüîìó 
ñêîğèãóéòå äàí³ òàêèì ÷èíîì, ùîá âîíè â³äïîâ³äàëè ğåàëüíèì ñóìàì òîâàğ³â ó çàìîâëåíí³*/

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


--Çàâäàííÿ 6: Ïğîåêòóâàííÿ ñõåìè 
/*Çàïğîïîíóéòå çì³íó ñòğóêòóğè áàçè äàíèõ äëÿ ïîêğàùåííÿ ïğîäóêòèâíîñò³ òà 
ìàñøòàáîâàíîñò³ â óìîâàõ øâèäêîãî çğîñòàííÿ ê³ëüêîñò³ çàìîâëåíü òà êë³ºíò³â (çà óìîâè 
îòğèìàííÿ 2ìëí. çàìîâëåíü íà äåíü). À òàêîæ çàïğîïîíóéòå ğîçøèğåííÿ ñõåìè äëÿ 
óìîæëèâëåííÿ ëîãóâàíü çì³í*/

-- 1. Â òàáëèö³ [dbo].[products] íå çáåğ³ãàòè çàëèøêè, ñòâîğèòè äëÿ íèõ îêğåìó òàáëèöş,
--    à òàáëèöş products âèêîğèñòîâóâàòè ÿê äîâ³äíèê òîâàğ³â, äîäàòè äî íå¿ ³íôîğìàö³ş ïğî ãğóïè òîâàğ³â, ïîñòà÷àëüíèê³â ³ ò.ä

-- 2. Â òàáëèö³ [dbo].[order_items] âèêîğèñòîâóâàòè íå [product_name] à [product_id], ÿê çîâí³øí³é êëş÷ íà products.id
--    öå áóäå ïîëåãøóâàòè îá'ºäíàòòÿ òàáëèöü, òà ïğèáèğàòè ìîæëèâ³ ïğîáëåìè ç îäíàêîâèìè íàçâàìè òîâàğó, ğåã³ñòğîì ³ ò.ä

-- 3. Äîäàòè ³íäåêñàö³ş íà òàáëèö³ [dbo].[orders] òà [dbo].[order_items], ùîá ïğèøâèäøèòè ğîáîòó ç òàáëèöÿìè, 
--    íàïğèêëàä íà êîëîíêó [dbo].[orders].[order_date] ùîá øâèäøå ä³ñòàâàòè çàìîâëåííÿ çà ä³àïàçîíîì äàò (ğåïîğòè, àíàë³òèêà)
--    òà [dbo].[order_items].[order_id] ùîá øâèäøå ğîáèòè äæîéíè äî orders.
--    Äîäàòêîâî ìîæíà [dbo].[orders].[customer_id] - çàìîâëåííÿ êë³ºíòà,
--    [dbo].[order_items].[product_id] - àíàë³ç ïî òîâàğàõ.

-- 4. Òàêîæ äîäàòè äî òàáëèöü [dbo].[orders] òà [dbo].[order_items] êîëîíêó [last_updated_at], äîçâîëèòü ğîçóì³òè ôğåøí³ñòü äàíèõ

-- 5. Äëÿ orders, order_items, products ñòâîğèòè òàáëèö³ *_history äëÿ ëîãóâàííÿ

-- 6. Òàêîæ ìîæíà ñòâîğşâàòè òàáëèö³ ç àãğåãàòàìè äëÿ àíàë³òèêè, ùîá êîæíîãî ğàçó íå ÷èòàò³ âñ³ ñèğ³ äàí³ ïî çàìîâëåííÿõ,
--    à íàïğèêëàä äîëèâàòè êîæíî¿ íî÷³ íîâ³ àãğåãîâàí³ äàí³ çà äîïîìîãîş ïğîöåäóğè

-- 7. Ç ÷àñîì ìîæíà ïîäóìàòè ïğî àğõ³âóâàíÿí ñòàğèõ äàíèõ

-- 8. ß á ùå äîäàëà òàáëèöş ç ³ñòîğ³ºş ö³í ïî äíÿõ, äëÿ îö³íêè çàëèøê³â, à òàêîæ òàáëèöş ç çàëèøêàìè íà äàòó


--Çàâäàííÿ 7: Ğîáîòà ç òèì÷àñîâèìè äàíèìè 
/*Íàïèø³òü SQL çàïèò äëÿ îá÷èñëåííÿ ïîì³ñÿ÷íîãî ïğèğîñòó íîâèõ êë³ºíò³â çà îñòàíí³é ğ³ê. 
Êğ³ì òîãî, âèçíà÷òå ñåğåäí³é â³äñîòîê ïğèğîñòó êë³ºíò³â çà ì³ñÿöÿìè.*/ 

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