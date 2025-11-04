declare @today date = cast(getdate() as date);
declare @six_months_ago date = dateadd(month, -6, @today); 
declare @age int = 30;                                     -- Вік
declare @numb_wc_compl int = 5;                            -- Кількість завершених активностей
declare @wo_act int = 1;                                   -- Акаунти без активності в додатку (міс) 
declare @subscription_duration int = 6;                    -- Тривалість підписки

with _events 
	as (
		select 
			 user_id      
		   , max(case
					when event_name = 'set_birthdate' 
					then try_convert(date, event_value, 23)      
				end) as event_birth_date
		   , sum(case 
					when event_name = 'workout_complete' and event_datetime >= @six_months_ago 
					then 1 
					else 0
				 end) as wc_count
		   , max(event_datetime) as last_event_at
		from test.dbo.app_events
		group by user_id
		)

select top (5000)
     idfa
from 
   test.dbo.installs     as f1
inner join
   test.dbo.purchases    as f2 on f1.user_id = f2.user_id and 
                                  f2.subscription_duration = @subscription_duration
inner join
   test.dbo.backend_data as f3 on f1.user_id = f3.user_id
left outer join
   _events               as f4 on f1.user_id = f4.user_id
where 
  1 = 1
  and upper(f1.country) in ('US', 'CA', 'AU', 'NZ', 'GB')
  and upper(f3.device_platform) = 'IOS'
  and coalesce(f3.user_birth_date, f4.event_birth_date) > dateadd(year, -@age, @today)
  and (f4.wc_count >= @numb_wc_compl
       or f4.last_event_at is null
       or f4.last_event_at < dateadd(month, -@wo_act, @today))
order by f2.revenue desc








