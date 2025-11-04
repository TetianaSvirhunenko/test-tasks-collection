with 
	campaigns
		as (
			select 
			     id
			   , group_id
			from
			   keitaro.keitaro_campaigns
			where group_id in (1065, 1039, 1038, 1037)
			),

	clicks 
		as (
			select
                 group_id  
               , date_key
               , sub_id
               , is_lead
               , is_sale
            from 
               keitaro.keitaro_clicks as f1 FINAL 
            inner join campaigns as f2 on f1.campaign_id = f2.id
		    ),

	conversions 
		as (
			select
			     sub_id
			   , sub_id_14 as gadid
			from 
			   keitaro.keitaro_conversions as f1
			inner join campaigns as f2 on f1.campaign_id = f2.id
			group by 1, 2)
							
	-- 130,768
select 
     f2.gadid
   , case 
       when f1.group_id = 1037 then 'Michelangelo'
       when f1.group_id = 1065 then 'Splinter'
       when f1.group_id = 1039 then 'Raphael'
       when f1.group_id = 1038 then 'Leonardo'
     end as campaign_group
   , max(f1.is_lead) as is_lead
   , max(f1.is_sale) as is_sale
from 
   clicks as f1
inner join 
   conversions as f2 on f1.sub_id = f2.sub_id
where date_key between '2025-05-22' and '2025-05-29'
group by 1, 2

