from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, to_timestamp, lpad, concat_ws, split
import pandas as pd
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("test_task").getOrCreate()

sheet_url = "https://docs.google.com/spreadsheets/d/1kghrW2qPYCa1wl7BIJgA08mrS2MkUelfn1SocL1jWDk/export?format=csv&gid=234859813"
pd_subscription_events = pd.read_csv(sheet_url, decimal=",")
pd_subscription_events["event_timestamp"] = pd.to_datetime(pd_subscription_events["event_timestamp"])

subscription_events = spark.createDataFrame(pd_subscription_events)

subscription_events.createOrReplaceTempView("subscription_events")


result_task_1_1 = spark.sql("""
    with subscription_events_grouped (
        select
            f1.uuid,	
            f1.event_timestamp,	
            f1.event_name,	
            f1.product_id,	
            f1.is_trial,	
            f1.period,	
            f1.trial_period,
            f1.revenue_usd,
            f1.transaction_id,
            sum(
                case 
                    when f1.event_name IN ('trial', 'cancellation') then 1
                    else 0
                end
            ) over (partition by f1.uuid, f1.product_id order by f1.event_timestamp rows unbounded preceding)  as reset_group  
        from 
            subscription_events as f1
        left outer join 
            subscription_events as f2 on f1.transaction_id = f2.refunded_transaction_id
        where f2.revenue_usd is null)

    select 
        uuid,
        product_id,
        event_timestamp,
        transaction_id,
        first_value(transaction_id) over (partition by uuid, product_id, reset_group order by event_timestamp) as original_transaction_id,
        revenue_usd,
        row_number() over (partition by uuid, product_id, reset_group order by event_timestamp)                as renewal_number  
    from 
        subscription_events_grouped
    where event_name in ('purchase', 'trial')
    order by uuid                                
    """)


                        

result_task_1_2 = spark.sql("""
    with uuid_agr as (    
        select 
            uuid,
            max(struct(event_timestamp, product_id, event_name, period))                           as last_event,       
            min(case when event_name = 'trial' then event_timestamp end)                           as trial_started_time,
            min(case when event_name = 'purchase' then event_timestamp end)                        as first_purchase_time,
            max(case when event_name = 'purchase' then event_timestamp end)                        as last_purchase_time,  
            count(case when event_name = 'purchase' then event_timestamp end)                      as total_purchases, 
            sum(revenue_usd)                                                                       as total_revenue_usd,
            max(case when event_name = 'cancellation' then event_timestamp end)                    as cancelation_time,
            max(case when event_name = 'refund' then event_timestamp end)                          as refund_time      
        from subscription_events         
        group by uuid)
    
    select 
        uuid,
        last_event.product_id as current_product_id,
        trial_started_time,
        first_purchase_time,
        last_purchase_time,
        total_purchases, 
        total_revenue_usd,
        last_purchase_time + make_interval(0,0,0,last_event.period,0,0,0)                          as expiration_time,
        cancelation_time,
        refund_time          
    from uuid_agr

                       """)

df_result_task_1_1 = result_task_1_1.toPandas()
df_result_task_1_1.to_excel("result_task_1_1.xlsx", index=False)

df_result_task_1_2 = result_task_1_2.toPandas()
df_result_task_1_2.to_excel("result_task_1_2.xlsx", index=False)


result_task_2 = spark.sql("""
    with purchases as (    
            select
                f1.uuid,
                first_value(date_trunc('month', f1.event_timestamp)) over (partition by f1.uuid order by f1.event_timestamp) as cohort_date,
                date_trunc('month', f1.event_timestamp) as event_date,
                f1.event_name,
                f1.product_id,	
                f1.is_trial,
                f1.period,
                f1.trial_period,
                f1.revenue_usd,
                f1.transaction_id
            from 
                subscription_events as f1
            left outer join 
                subscription_events as f2 on f1.transaction_id = f2.refunded_transaction_id
            where f1.event_name in ('purchase')
              and f2.revenue_usd is null
            ),
                          
    cohorts as (
        select distinct
            cohort_date,
            count(distinct uuid)                       as cohort_user_count
        from 
            purchases  
        group by cohort_date),
    
    arpu as (
        select distinct
            f1.cohort_date,
            f1.event_date,
            f2.cohort_user_count,
            sum(f1.revenue_usd)                        as revenue_usd,
            sum(f1.revenue_usd) / f2.cohort_user_count as arpu
        from 
            purchases as f1
        left outer join 
            cohorts as f2 on f1.cohort_date = f2.cohort_date
        where f1.cohort_date is not null  
        group by f1.cohort_date, f1.event_date, f2.cohort_user_count) 
                         
    select
        cohort_date,
        event_date,
        revenue_usd,
        cohort_user_count,
        arpu,
        sum(arpu) over (partition by cohort_date order by event_date) as cumulative_ltv
    from 
        arpu  
      
                            """)

df_result_task_2 = result_task_2.toPandas()
df_result_task_2.to_excel("result_task_2.xlsx", index=False)
