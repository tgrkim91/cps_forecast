drop table if exists #attribution_v3_signups;
select driver_id,
       signup_date,
       signup_month,
       case when country='AU' and channel_lvl_5 in ('Google_Desktop','Google_Mobile') then 'Google'
            when country='AU' and channel_lvl_5 in ('Google_Desktop_Brand','Google_Mobile_Brand') then 'Google_Brand'
            else channel_lvl_5 end as channels,
       platform,
       country,
       2 as pick_rank
into #attribution_v3_signups
from marketing.marketing_channel_by_driver
where country in ('US','GB','CA','AU')
and channel_lvl_5<>'Unknown';


drop table if exists #data_warehouse_signups;
select ddi.driver_id,
       dd.date as signup_date,
       date_trunc('month', dd.date)::d as signup_month,
       cd.channel_name as channels,
       sf.platform,
       ld.country_code as country
into #data_warehouse_signups
from warehouse.session_fact sf
join warehouse.date_dim dd
    on sf.session_date_pdt = dd.date
join warehouse.location_dim ld
    on sf.source_location_key = ld.location_key
join warehouse.channel_dim cd
    on sf.channel_key = cd.channel_key
join warehouse.sign_up_conversion_mart sucf
    on sf.session_key = sucf.session_key
join warehouse.driver_dim ddi
    on sucf.driver_key=ddi.driver_key
where true
    and sucf.rank_desc_paid_90_days = 1
;

drop table if exists #data_warehouse_signups_pick;
select *,
       1 as pick_rank
into #data_warehouse_signups_pick
from #data_warehouse_signups
where 1=1
    and country = 'US'
    and channels = 'Apple_Brand'
;

drop table if exists #temp_signup_base;
select *
into #temp_signup_base
from
    (select *,
           row_number() over (partition by driver_id order by pick_rank) as rank1
    from
        (select *
            from #attribution_v3_signups
        union all
            (select * from #data_warehouse_signups_pick)))
where rank1=1;

---------------------------------trip days from data science model---------------------------------------
--prediction for US only
drop table if exists #temp_signup_base_us_only;
select *
into #temp_signup_base_us_only
from #temp_signup_base
where country='US';

--ltr
drop table if exists #ltr;
select a.driver_id
     , b.signup_date
     , b.signup_month
     , b.channels
     , a.days_since_first_trip_end/28 as month_since_first_trip
     , a.ltr
     ,date_trunc('day',a.prediction_date)::D as prediction_date
into #ltr
from
(select *
     from marketing.ltr_from_activation_last
     where run_id in (SELECT run_id FROM marketing_scratch.ltr_run_id)
      ) a

inner join
(select date_trunc('month',prediction_date) as prediction_month,
        max(date_trunc('day',prediction_date)) as prediction_date
 from marketing.ltr_from_activation_last
where run_id in (SELECT run_id FROM marketing_scratch.ltr_run_id)
 group by 1) c
on date_trunc('day',a.prediction_date)=c.prediction_date

inner join #temp_signup_base_us_only b
on a.driver_id=b.driver_id

inner join marketing.ltr_from_activation_historical lha
on lha.driver_id=a.driver_id AND lha.run_id=a.run_id
--averaging by signup day, with 30 day offset
where DATEDIFF(day, b.signup_date, date_trunc('day',a.prediction_date))<=60 and DATEDIFF(day, b.signup_date, date_trunc('day',a.prediction_date))>=30;

drop table if exists #ltr1;
select channels,
       date_trunc('month',dateadd('month',2,prediction_date)) as estimation_month,
       month_since_first_trip,
       avg(ltr) as ltr_per_activation
into #ltr1
from #ltr
group by 1,2,3;

--activation
drop table if exists #activation;
select
         a.driver_id
       , b.signup_date
       , b.signup_month
       , b.channels
       , a.pred_day_after_signup/28 as month_since_signup
       , a.pred_p as activate_prob
       , date_trunc('day',a.created)::D as prediction_date
into #activation
from
(select driver_id,
        pred_day_after_signup,
        pred_p,
        case when date_trunc('day',created)='2021-10-01' then '2021-09-30' 
             when date_trunc('day',created)='2023-09-01' then '2023-08-31' else created end as created
 from marketing.prediction_driver_activation_curve
 where pred_day_after_signup%28=0 and pred_day_after_signup<>0
    and pred_p>=0 and pred_p is not null
    and model_version IN (SELECT model_version FROM marketing_scratch.activation_run_id)
 ) a

 inner join
 (select date_trunc('month',created) as prediction_month,
         max(date_trunc('day',created)) as prediction_date
  from (select case when date_trunc('day',created)='2021-10-01' then '2021-09-30'
                    when date_trunc('day',created)='2023-09-01' then '2023-08-31' else created end as created
        from marketing.prediction_driver_activation_curve
        where model_version IN (SELECT model_version FROM marketing_scratch.activation_run_id))
  group by 1) c
 on date_trunc('day',a.created)=c.prediction_date

inner join #temp_signup_base_us_only b
on a.driver_id=b.driver_id
where DATEDIFF(day, b.signup_date, date_trunc('day',a.created))<=60 and DATEDIFF(day, b.signup_date, date_trunc('day',a.created))>=30;--averaging by signup date, with 30 day offset


drop table if exists #activation1;
select channels,
       estimation_month,
       month_since_signup,
       cumulative_activate_rate,
       (case when month_since_signup>1 then cumulative_activate_rate-previous_activate_rate else cumulative_activate_rate end) as incremental_activate_rate
into #activation1
from
    (select *,
           lag(cumulative_activate_rate,1) over (partition by channels,estimation_month order by month_since_signup) as previous_activate_rate
    from
        (select channels,
               date_trunc('month',dateadd('month',2,prediction_date)) as estimation_month,
               month_since_signup,
               avg(activate_prob) as cumulative_activate_rate
        from #activation
        group by 1,2,3));


--ds_master
drop table if exists #activation3;
select a.channels,
       a.signup_month,
       a.num_signups,
       b.month_since_signup,
       dateadd('month',b.month_since_signup-1,a.signup_month) as activation_month,
       b.incremental_activate_rate,
       a.num_signups*b.cumulative_activate_rate as cumulative_activations,
       a.num_signups*b.incremental_activate_rate as activations
into #activation3
from (select *
      from (select channels,
                   signup_month,
                   count(1) as num_signups
            from #temp_signup_base_us_only
            group by 1, 2)
            union all
            (select distinct channels,
                    dateadd('month',1,date_trunc('month',CURRENT_DATE)) as signup_month,
                    1000 as num_signups
            from #temp_signup_base_us_only)
     ) a
left join #activation1 b
on a.channels=b.channels
and a.signup_month=b.estimation_month;


drop table if exists #ds_master;
select a.channels,
       a.signup_month,
       a.num_signups,
       a.month_since_signup,
       a.activation_month,
       a.cumulative_activations,
       a.activations,
       b.month_since_first_trip as month_since_activation,
       b.ltr_per_activation,
       dateadd('month',b.month_since_first_trip-1,a.activation_month) as ltr_month,
       datediff('month',a.signup_month,dateadd('month',b.month_since_first_trip-1,a.activation_month)) as ltr_month_since_singup,
       a.activations*b.ltr_per_activation as ltr,
       a.activations*b.ltr_per_activation/a.num_signups as ltr_per_signup
into #ds_master
from #activation3 a
left join #ltr1 b
on a.channels=b.channels
and a.signup_month=b.estimation_month;


drop table if exists #ds_final;
select a.*,
       b.activations,
       a.accumulated_ltr_per_signup-nvl(lag(a.accumulated_ltr_per_signup,1) over (partition by a.channels,a.signup_month order by a.increments_from_signup),0) as ltr_per_signup
into #ds_final
from
    (select channels,
           signup_month,
           ltr_month_since_singup+1 as increments_from_signup,
           ltr_month as trip_month,
           sum(ltr_per_signup) as accumulated_ltr_per_signup
    from #ds_master
    where ltr_month_since_singup+1<=24
    group by 1,2,3,4) a
join
    (select channels,
           signup_month,
           month_since_signup as increments_from_signup,
           avg(activations) as activations
    from #ds_master
    where ltr_month_since_singup+1<=24
    group by 1,2,3) b
on a.channels=b.channels
and a.signup_month=b.signup_month
and a.increments_from_signup=b.increments_from_signup;

--------------net revenue per trip day of activation trip---------
drop table if exists #net_revenue_of_activation_trip;
select a.channels,
       a.signup_month,
       sum(b.gaap_net_revenue::float) as gaap_net_revenue,
       sum(b.paid_days::float) as paid_days,
       sum(b.gaap_net_revenue::float)/sum(b.paid_days::float) as net_revenue_per_day
into #net_revenue_of_activation_trip
from
    (select driver_id,
           channels,
           date_trunc('month',dateadd('month',1,prediction_date)) as signup_month
    from #ltr
    group by 1,2,3) a
left join
    (select * from
        (select rs.driver_id,
           rs.reservation_id,
           d.gaap_net_revenue,
           e.paid_days,
           row_number() over (partition by rs.driver_id order by rs.trip_start_ts) as rank1
            FROM analytics.reservation_summary rs
              inner join (select distinct driver_id from #ltr) b
              on rs.driver_id=b.driver_id
            left join finance.reservation_profit_summary_staging d
                   on rs.reservation_id=d.reservation_id
            left join analytics.reservation_dimensions e
                   on rs.reservation_id=e.reservation_id
             WHERE 1=1
            and rs.is_ever_booked=1 and rs.current_status not in (2,11)
            and rs.trip_start_ts is not null)
    where rank1=1) b
on a.driver_id=b.driver_id
group by 1,2
order by 1,2;

------------paid days per signup----------------
drop table if exists #paid_days_per_signup_ds;
select a.channels,
       a.signup_month,
       a.increments_from_signup,
       a.activations,
       a.trip_month,
       a.ltr_per_signup,
       b.net_revenue_per_day,
       a.ltr_per_signup::float/b.net_revenue_per_day::float as paid_days_per_signup
into #paid_days_per_signup_ds
from #ds_final a
left join #net_revenue_of_activation_trip b
on a.channels=b.channels and a.signup_month=b.signup_month
order by 1,2,3;

DROP TABLE IF EXISTS #ds_curve_by_signup_month_cps;
WITH pcp_denom AS (
    SELECT *,
           LAG(paid_days_per_signup) OVER (PARTITION BY channels,signup_month ORDER BY increments_from_signup) AS previous_paid_days_per_signup
    FROM #paid_days_per_signup_ds
)

select * INTO #ds_curve_by_signup_month_cps from
(SELECT channels,
       signup_month,
       increments_from_signup,
       activations,
       sum(activations) over (partition by channels,signup_month order by increments_from_signup ROWS UNBOUNDED PRECEDING) as accumulated_activations,
       paid_days_per_signup,
       previous_paid_days_per_signup,
       CASE WHEN increments_from_signup=1 THEN 1 ELSE paid_days_per_signup::FLOAT /previous_paid_days_per_signup::FLOAT END AS ds_curve
FROM pcp_denom)
;

--adjust seasonal factor
DROP TABLE IF EXISTS #actual_2_increments;

select *,
       paid_days_per_signup_original*seasonal_index as paid_days_per_signup
into #actual_2_increments
from
    (select channels,
           signup_month,
           increments_from_signup,
           paid_days::float/signups::float as paid_days_per_signup_original,
           case when date_part('month',signup_month)=1 then 0.93
                when date_part('month',signup_month)=2 then 0.92
                when date_part('month',signup_month)=3 then 1.00
                when date_part('month',signup_month)=4 then 1.07
                when date_part('month',signup_month)=5 then 1.07
                when date_part('month',signup_month)=6 then 1.02
                when date_part('month',signup_month)=7 then 1.06
                when date_part('month',signup_month)=8 then 1.1
                when date_part('month',signup_month)=9 then 1.06
                when date_part('month',signup_month)=10 then 0.95
                when date_part('month',signup_month)=11 then 0.92
                when date_part('month',signup_month)=12 then 0.93
           else null end as seasonal_index
         from tableau.historical_master
         where country='US' and increments_from_signup<=2)
;

--need to update join on signup_month, already did
DROP TABLE IF EXISTS #demand_paid_days_cps;
SELECT b.channels,
       b.signup_month,
       b.increments_from_signup,
       b.activations,
       b.accumulated_activations,
       b.paid_days_per_signup,
       b.ds_curve,
       a.paid_days_per_signup*1 AS projected_paid_days
INTO #demand_paid_days_cps
FROM #actual_2_increments a
         RIGHT JOIN (SELECT * FROM #ds_curve_by_signup_month_cps
                    WHERE increments_from_signup<=12
                    and signup_month<>'2023-06-01') b --no ltr model for June 2023
              ON  a.channels=b.channels
              AND a.signup_month = dateadd('month',-3,b.signup_month) --pay attention
              AND a.increments_from_signup = b.increments_from_signup;

select *
from #demand_paid_days_cps
where channels in ('Apple','Google_Desktop','Google_Desktop_Brand','Google_Discovery',
    'Google_Mobile','Google_Mobile_Brand','Google_UAC_Android','Kayak_Desktop',
    'Kayak_Desktop_Core', 'Kayak_Mobile_Core','Mediaalpha','Expedia','Microsoft_Desktop',
    'Microsoft_Desktop_Brand','Microsoft_Mobile', 'Microsoft_Mobile_Brand','Kayak_Desktop_Front_Door',
    'Kayak_Desktop_Compare','Google_Pmax','Kayak_Desktop_Carousel','Kayak_Mobile_Carousel',
    'Kayak_Mobile','Kayak_Afterclick', 'Facebook/IG_App', 'Facebook/IG_Web');