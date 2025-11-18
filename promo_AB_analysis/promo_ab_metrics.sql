drop table if exists test.dbo.promo_metrics;

with cleaned_data as (
	select 
		 promoid
	   , CG
	   , memberid
	   , created
	   , chequeid
	   , sumcheq
	   , sumreg
	   , max(flag_lagercheque) as flag_lagercheque
	 from
		 test.dbo.promo_data
	 group by promoid, CG, memberid, chequeid, created, sumcheq, sumreg
	 ),

duplicates AS (
    select 
        chequeid
    from cleaned_data
    where chequeid is not null
    group by chequeid
    having count(distinct promoid) > 1
	),

base as (
	select 
		 promoid
	   , count(distinct case when CG = 1 then memberid end )                            as customers_test
	   , count(distinct case when CG = 2 then memberid end )                            as customers_ctrl
	   , count(distinct case when chequeid is not null and CG = 1 THEN memberid end)    as buyers_test
	   , count(distinct case when chequeid is not null and CG = 2 THEN memberid end)    as buyers_ctrl
	   , count(distinct case when CG = 1 then chequeid end)                             as checks_test
	   , count(distinct case when CG = 2 then chequeid end)                             as checks_ctrl
	   , isnull(sum(case when CG = 1 then sumcheq end), 0)                              as gross_test
	   , isnull(sum(case when CG = 2 then sumcheq end), 0)                              as gross_ctrl
	   , isnull(sum(case when CG = 1 then sumreg end), 0)                               as revenue_test
	   , isnull(sum(case when CG = 2 then sumreg end), 0)                               as revenue_ctrl
	from cleaned_data
	group by promoid
	),

base_no_dups  as (
	select 
		 promoid
	   , count(distinct case when CG = 1 then memberid end )                            as customers_test_no_dup
	   , count(distinct case when CG = 2 then memberid end )                            as customers_ctrl_no_dup
	   , count(distinct case when f1.chequeid is not null and CG = 1 THEN memberid end) as buyers_test_no_dup
	   , count(distinct case when f1.chequeid is not null and CG = 2 THEN memberid end) as buyers_ctrl_no_dup
	   , count(distinct case when CG = 1 then f1.chequeid end)                          as checks_test_no_dup
	   , count(distinct case when CG = 2 then f1.chequeid end)                          as checks_ctrl_no_dup
	   , isnull(sum(case when CG = 1 then sumcheq end), 0)                              as gross_test_no_dup
	   , isnull(sum(case when CG = 2 then sumcheq end), 0)                              as gross_ctrl_no_dup
	   , isnull(sum(case when CG = 1 then sumreg end), 0)                               as revenue_test_no_dup
	   , isnull(sum(case when CG = 2 then sumreg end), 0)                               as revenue_ctrl_no_dup
	from cleaned_data as f1
	left outer join duplicates as f2 ON f1.chequeid = f2.chequeid
	where f2.chequeid is null
	group by promoid
	)

select 
     f1.promoid
	 -- with duplicates
   , f1.customers_test
   , f1.customers_ctrl
   , f1.buyers_test
   , f1.buyers_ctrl
   , f1.checks_test
   , f1.checks_ctrl
   , f1.gross_test
   , f1.gross_ctrl
   , f1.revenue_test
   , f1.revenue_ctrl
   , isnull(cast(f1.buyers_test as float) / nullif(customers_test, 0), 0) as CR_test
   , isnull(cast(f1.buyers_ctrl as float) / nullif(customers_ctrl, 0), 0) as CR_ctrl
   , isnull(f1.revenue_test / nullif(checks_test, 0), 0) AS AOV_test
   , isnull(f1.revenue_ctrl / nullif(checks_ctrl, 0), 0) AS AOV_ctrl
   -- without duplicates
   , f2.customers_test_no_dup
   , f2.customers_ctrl_no_dup
   , f2.buyers_test_no_dup
   , f2.buyers_ctrl_no_dup
   , f2.checks_test_no_dup
   , f2.checks_ctrl_no_dup
   , f2.gross_test_no_dup
   , f2.gross_ctrl_no_dup
   , f2.revenue_test_no_dup
   , f2.revenue_ctrl_no_dup
   , isnull(cast(f2.buyers_test_no_dup as float) / nullif(customers_test_no_dup, 0), 0) as CR_test_no_dup
   , isnull(cast(f2.buyers_ctrl_no_dup as float) / nullif(customers_ctrl_no_dup, 0), 0) as CR_ctrl_no_dup
   , isnull(f2.revenue_test_no_dup / nullif(checks_test_no_dup, 0), 0) AS AOV_test_no_dup
   , isnull(f2.revenue_ctrl_no_dup / nullif(checks_ctrl_no_dup, 0), 0) AS AOV_ctrl_no_dup
   , ((f1.checks_test + f1.checks_ctrl) - (f2.checks_test_no_dup + f2.checks_ctrl_no_dup))/ nullif(cast((f1.checks_test + f1.checks_ctrl) as float), 0) as dup_check_share
into test.dbo.promo_metrics  
from base as f1
left outer join base_no_dups as f2 on f1.promoid = f2.promoid
order by f1.promoid




