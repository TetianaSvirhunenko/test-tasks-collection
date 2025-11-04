with 
to_exclude
	as (
		select 
		     gadid
		   , count(distinct tag) as tag_qty
		from
		default.device FINAL
		where tag is not null
		  and type = 1
		  and active = true
		group by 1
		having tag_qty > 1
		),

devices 
	as (
		select 
		     gadid
		   , id
		   , tag 
		   , country_name
		from
		default.device FINAL
		where tag is not null
		  and type = 1
		  and active = true),
		
events 
   as (
	select 
	     device_id
	   , count(*) as push_qty
	from
	default.event 
	where toDate(created_at) BETWEEN '2025-05-22' and '2025-05-29'
	  and event_type = 7
	  and sub_1 = 1
	group by 1)

	
	-- 591,784
select distinct
     gadid
   , tag 
   , country_name
   , push_qty
from 
	devices as f2 
left outer join 
    events as f1 on f1.device_id = f2.id
where f2.gadid not in (select gadid from to_exclude) 
  and (push_qty > 0
   or (tag = '6' and push_qty = 0))
  

