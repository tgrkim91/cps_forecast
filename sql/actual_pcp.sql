DROP TABLE IF EXISTS #increments;
SELECT TOP 60 --Why are we selecting 60 increments?
       ROW_NUMBER () OVER () AS increments_from_signup
INTO #increments
FROM analytics.driver_summary
;

------------------------------------------------------------------------------------------------------------------------
--Signups
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #attribution_v3_signups;
SELECT driver_id,
       signup_date,
       signup_month,
       channel_lvl_5 as channels, --In future, should rename as "channel"
       paid_halo,
       2 as pick_rank
INTO #attribution_v3_signups
FROM marketing.marketing_channel_by_driver
WHERE signup_month BETWEEN '2016-01-01' AND DATEADD(month, -1, DATE_TRUNC('month', CURRENT_DATE))::D
AND country IN ('US')
AND channel_lvl_5 IN ('Apple','Google_Desktop','Google_Desktop_Brand','Google_Discovery',
                      'Google_Mobile','Google_Mobile_Brand','Google_UAC_Android','Kayak_Desktop','Kayak_Desktop_Core',
                      'Kayak_Mobile_Core','Mediaalpha','Expedia','Microsoft_Desktop','Microsoft_Desktop_Brand','Microsoft_Mobile',
                      'Microsoft_Mobile_Brand','Kayak_Desktop_Front_Door','Kayak_Desktop_Compare','Google_Pmax','Kayak_Desktop_Carousel','Kayak_Mobile_Carousel',
                      'Kayak_Mobile','Kayak_Afterclick')
;

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
select driver_id,
       signup_date,
       signup_month,
       channels,
       0.415 as paid_halo,
       1 as pick_rank
into #data_warehouse_signups_pick
from #data_warehouse_signups
where 1=1
    and country = 'US'
    and channels = 'Apple_Brand'
    and signup_month BETWEEN '2016-01-01' AND DATEADD(month, -1, DATE_TRUNC('month', CURRENT_DATE))::D
;

drop table if exists #signups;
select *
into #signups
from
    (select *,
           row_number() over (partition by driver_id order by pick_rank) as rank1
    from
        (select *
            from #attribution_v3_signups
        union all
            (select * from #data_warehouse_signups_pick)))
where rank1=1;

DROP TABLE IF EXISTS #signups_tot;
SELECT channels
     , signup_month
     , COUNT(DISTINCT driver_id) AS signups
INTO #signups_tot
FROM #signups
WHERE TRUE
GROUP BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------
--Cohort Detail
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #cohort_detail;
SELECT s.channels
     , s.signup_month
     --, DATEDIFF(month, s.signup_month, DATE_TRUNC('month', d.date)) + 1                                                  AS increments_from_signup
     --Is the datediff 30 consistent with what Jodie uses? We have 30 days for actuals, and 28 days for predictions?
     , FLOOR(DATEDIFF(day, s.signup_date, d.date::date)/30) + 1                                                          AS increments_from_signup
     , SUM(CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END)                       AS paid_days
     , SUM(CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END / rd.paid_days::FLOAT) AS trips
     , SUM(rps.gaap_net_revenue / rd.paid_days::FLOAT)                                                                   AS net_revenue_usd
     , SUM(rps.partial_contribution_profit / rd.paid_days::FLOAT)                                                        AS actual_partial_contribution_profit_usd
     , SUM(CASE WHEN COALESCE(rs.trip_end_ts, rs.current_end_ts)::Date <= DATEADD(month, -4, DATE_TRUNC('month', CURRENT_DATE))::Date THEN (rps.partial_contribution_profit - rps.gaap_net_revenue * 0.02)
                ELSE (rps.gaap_net_revenue * 0.98 - cp.total_cost_per_trip_day * rd.paid_days)
           END / rd.paid_days::FLOAT)                                                                                    AS partial_contribution_profit_usd_no_halo
     , SUM(CASE WHEN COALESCE(rs.trip_end_ts, rs.current_end_ts)::Date <= DATEADD(month, -4, DATE_TRUNC('month', CURRENT_DATE))::Date THEN (rps.partial_contribution_profit - rps.gaap_net_revenue * 0.02) * s.paid_halo --@jodie: why is default halo 1.25? A: because most channels are 0.25 halo effect. NOTE: This is entirely arbitrary and should be revisited :).
                ELSE (rps.gaap_net_revenue * 0.98 - cp.total_cost_per_trip_day * rd.paid_days) * s.paid_halo -- The *0.98 is acceptable on an overall basis, should be updated for user level (e.g. 98% of all net revenue for a month, amortized uniformly across bookings)
           END / rd.paid_days::FLOAT)                                                                                    AS partial_contribution_profit_usd  -- on average, it takes about 3 months for all costs to get realized, so we use estimated cost_per_trip_day to compute the contribution
           -- There are two issues with cp.total_cost_per_trip_day : it is updating everytime (no consistent historical value) + does not account for cost per day fluctuations over time
     , SUM(csr.paid_claims / rd.paid_days::FLOAT)                                                                        AS paid_claims
INTO #cohort_detail
FROM finance.reservation_profit_summary_staging rps
         JOIN analytics.reservation_summary rs
              ON rps.reservation_id = rs.reservation_id
         JOIN (SELECT *,
               CASE WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.01 THEN 'A1'
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0045 THEN 'A1'
                    WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.02 THEN 'A2'
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.009 THEN 'A2'
                    WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.03 THEN 'A3'   
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0135 THEN 'A3' 
                    WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.06 THEN 'B'
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0315 THEN 'B'       
                    WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.09 THEN 'C'
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.05 THEN 'C'       
                    WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.12 THEN 'D'
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.07 THEN 'D'       
                    WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.18 THEN 'E'
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.10 THEN 'E'    
                    WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco >= 0.18 THEN 'F'
                    WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco > 0.10 THEN 'F'       
                    WHEN monaco IS NULL THEN 'NA'
                   ELSE 'NA' END as monaco_bin
               FROM analytics.reservation_dimensions) rd
              ON rps.reservation_id = rd.reservation_id
         JOIN #signups s
              ON rs.driver_id = s.driver_id
         LEFT JOIN analytics.claim_summary_rebuilt csr
              ON rps.reservation_id = csr.reservation_id
         LEFT JOIN marketing_scratch.cost_per_trip_day_by_monaco_demand cp
              ON rd.monaco_bin=cp.monaco_bin
         JOIN analytics.date d
            -- Is the idea here to include a single day for each date of a trip?
              ON d.date BETWEEN COALESCE(rs.trip_start_ts, rd.current_start_ts)::D AND DATEADD('day', rd.paid_days::INT - 1, COALESCE(rs.trip_start_ts, rs.current_start_ts))::D
WHERE TRUE
  --AND d.date < DATE_TRUNC('month', CURRENT_DATE)
        -- Converting to ::Date redundant, low priority
        -- This should ensure we are only observing trip days that have actually occurred
      AND DATEADD('month', CAST(increments_from_signup - 1 AS int), s.signup_month)::Date < DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))::Date
GROUP BY 1, 2, 3
;

------------------------------------------------------------------------------------------------------------------------
--Cohort Summary
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #marketing_cohort_metrics; -----------actual net_revenue_usd, partial_contribution_profit_usd, per channel, signup_month
SELECT s.channels
     , s.signup_month
     , i.increments_from_signup
     , s.signups
     , NVL(cd.paid_days, 0)::FLOAT                AS paid_days
     , NVL(cd.trips, 0)                           AS trips
     , NVL(cd.net_revenue_usd, 0)                 AS net_revenue_usd
     , NVL(cd.actual_partial_contribution_profit_usd,0) AS actual_partial_contribution_profit_usd
     , NVL(cd.partial_contribution_profit_usd_no_halo, 0) AS partial_contribution_profit_usd_no_halo
     , NVL(cd.partial_contribution_profit_usd, 0) AS partial_contribution_profit_usd
     , NVL(cd.paid_claims, 0)                     AS paid_claims
INTO #marketing_cohort_metrics
FROM #signups_tot s
         JOIN #increments i
              ON i.increments_from_signup BETWEEN 1 AND 60
         LEFT JOIN #cohort_detail cd
                   ON s.channels = cd.channels
                       AND s.signup_month = cd.signup_month
                       AND i.increments_from_signup = cd.increments_from_signup
WHERE TRUE
;

-----------------------Trip days --------------------------------
DROP TABLE if exists #ltr;
SELECT *
INTO #ltr
FROM
    (SELECT a.driver_id
         , b.signup_month
         , b.channels
         , b.paid_halo
         -- Where does the first trip end get considered? Is it populated as 0? Want to make sure we're considering that value here
         , a.days_since_first_trip_end/28 AS month_since_first_trip --Issue in how we're comparing...in theory with the first 2 "months" of 30 days we can scale effectively but still iffy.
         , a.ltr
         , DATE_TRUNC('day',a.prediction_date)::D AS prediction_date
         , RANK() OVER (PARTITION BY a.driver_id ORDER BY DATE_TRUNC('day',a.prediction_date) DESC) AS rank1
    FROM
    (SELECT *
         FROM marketing.ltr_from_activation_last --Confirm with @Jodie that this has been updated
         WHERE run_id IN (SELECT run_id FROM marketing_scratch.ltr_run_id)
          ) a

    INNER JOIN
    (SELECT DATE_TRUNC('month',prediction_date)    AS prediction_month,
            MAX(DATE_TRUNC('day',prediction_date)) AS prediction_date
     FROM marketing.ltr_from_activation_last
     WHERE run_id IN (SELECT run_id FROM marketing_scratch.ltr_run_id)
     GROUP BY 1) c
    ON DATE_TRUNC('day',a.prediction_date)=c.prediction_date

    INNER JOIN #signups b
    ON a.driver_id=b.driver_id --Only includes predictions for drives who've signed up

    INNER JOIN marketing.ltr_from_activation_historical lha --What is the purpose of this join?
    ON lha.driver_id=a.driver_id AND lha.run_id=a.run_id
    --averaging by signup day, with 30 day offset
    -- ^^ Signup month?
    WHERE DATEDIFF(day, b.signup_date, date_trunc('day',a.prediction_date))<=60 AND DATEDIFF(day, b.signup_date, DATE_TRUNC('day',a.prediction_date))>=30)
WHERE rank1=1
;

DROP TABLE IF EXISTS #ltr1;
SELECT channels,
       signup_month,
       month_since_first_trip,
       --avg(ltr*COALESCE(1 + NULLIF(s.paid_halo, 0), 1.25)) AS ltr_per_activation
       AVG(ltr) AS ltr_per_activation --Why are we not using paid halo?
INTO #ltr1
FROM #ltr
GROUP BY 1,2,3;

--activation
DROP TABLE IF EXISTS #activation;
SELECT *
INTO #activation
FROM
    (SELECT
             a.driver_id  
           , b.signup_month
           , b.channels
           , a.pred_day_after_signup/28                                                       AS month_since_signup
           , a.pred_p                                                                         AS activate_prob
           , DATE_TRUNC('day',a.created)::D                                                   AS prediction_date
           --truncing a.created is redundant/unncessary
           , RANK() OVER (PARTITION BY a.driver_id ORDER BY DATE_TRUNC('day',a.created) DESC) AS rank1

    FROM
    (SELECT driver_id,
            pred_day_after_signup,
            pred_p,
            CASE WHEN DATE_TRUNC('day',created)='2021-10-01' THEN '2021-09-30' ELSE created END AS created
     FROM marketing.prediction_driver_activation_curve
     WHERE pred_day_after_signup%28=0 AND pred_day_after_signup<>0
        AND pred_p>=0 AND pred_p IS NOT null
        AND model_version IN (SELECT model_version FROM marketing_scratch.activation_run_id)
            ) a

     INNER JOIN
     (SELECT DATE_TRUNC('month',created)    AS prediction_month,
             MAX(DATE_TRUNC('day',created)) AS prediction_date
      FROM (SELECT CASE WHEN DATE_TRUNC('day',created)='2021-10-01' THEN '2021-09-30' ELSE created END AS created
            FROM marketing.prediction_driver_activation_curve
            WHERE model_version IN (SELECT model_version FROM marketing_scratch.activation_run_id))
      GROUP BY 1) c
     ON DATE_TRUNC('day',a.created)=c.prediction_date

    INNER JOIN #signups b
    ON a.driver_id=b.driver_id
    WHERE DATEDIFF(day, b.signup_date, date_trunc('day',a.created))<=60 AND DATEDIFF(day, b.signup_date, date_trunc('day',a.created))>=30)
WHERE rank1=1
;--averaging by signup date, with 30 day offset

DROP TABLE IF EXISTS #activation1;
SELECT channels,
       signup_month,
       month_since_signup,
       cumulative_activate_rate,
       (CASE WHEN month_since_signup>1 THEN cumulative_activate_rate-previous_activate_rate ELSE cumulative_activate_rate END) AS incremental_activate_rate
INTO #activation1
FROM
    (SELECT *,
           LAG(cumulative_activate_rate,1) OVER (PARTITION BY channels, signup_month ORDER BY month_since_signup) AS previous_activate_rate
     FROM
        (SELECT channels,
               signup_month,
               month_since_signup,
               AVG(activate_prob) AS cumulative_activate_rate
        FROM #activation
        GROUP BY 1,2,3));

--ds_master
DROP TABLE IF EXISTS #activation3;
SELECT a.channels,
       a.signup_month,
       a.num_signups,
       b.month_since_signup,
       DATEADD('month',b.month_since_signup-1,a.signup_month) AS activation_month,
       b.incremental_activate_rate,
       a.num_signups*b.cumulative_activate_rate               AS cumulative_activations,
       a.num_signups*b.incremental_activate_rate              AS activations
INTO #activation3
FROM (SELECT channels,
             signup_month,
             COUNT(1) AS num_signups
      FROM #signups
      GROUP BY 1,2) a
LEFT JOIN #activation1 b
--Join to signup_month/channel to get activations based on total signups
ON a.signup_month=b.signup_month AND a.channels = b.channels;

DROP TABLE IF EXISTS #ds_master;
SELECT a.channels,
       a.signup_month,
       a.num_signups,
       a.month_since_signup,
       a.activation_month,
       a.cumulative_activations,
       a.activations,
       b.month_since_first_trip                                                                        AS month_since_activation,
       b.ltr_per_activation,
       -- month_since_first_trip can be 0, so month_since_first_trip-1 can be -1 (ltr_month can be before activation_month?)
       DATEADD('month',b.month_since_first_trip-1,a.activation_month)                                  AS ltr_month,
       DATEDIFF('month',a.signup_month,DATEADD('month',b.month_since_first_trip-1,a.activation_month)) AS ltr_month_since_singup,
       a.activations*b.ltr_per_activation                                                              AS ltr,
       a.activations*b.ltr_per_activation/a.num_signups                                                AS ltr_per_signup
INTO #ds_master
-- The below join results in a cartesian product, producing a row at uniqueness of channel, signup_month, month_since_signup, month_since_first_trip
FROM #activation3 a
LEFT JOIN #ltr1 b
ON a.channels=b.channels
and a.signup_month=b.signup_month;

DROP TABLE IF EXISTS #ds_final;
SELECT a.*,
       b.activations,
       a.accumulated_ltr_per_signup-nvl(LAG(a.accumulated_ltr_per_signup,1) OVER (PARTITION BY a.channels,a.signup_month ORDER BY a.increments_from_signup),0) AS ltr_per_signup
INTO #ds_final
FROM
    (SELECT channels,
            signup_month,
            --Increment 1 is first month
            ltr_month_since_singup+1        AS increments_from_signup,
            ltr_month                       AS trip_month,
            -- Combine all activations at all time horizons for a given signup month to get the total LTR
            SUM(ltr_per_signup)             AS accumulated_ltr_per_signup
    FROM #ds_master
    WHERE ltr_month_since_singup+1<=24
    GROUP BY 1,2,3,4) a
JOIN
    (SELECT channels,
            signup_month,
            month_since_signup              AS increments_from_signup,
            --AVG here seems analogous to select distinct? Why don't we just pull from activations?
            AVG(activations)                AS activations
    FROM #ds_master
        --Why are we filtering on ltr_month_since_signup and not month_since_signup? seems like the filter below isn't achieving anything
    WHERE ltr_month_since_singup+1<=24
    GROUP BY 1,2,3) b
ON a.channels = b.channels
and a.signup_month=b.signup_month
AND a.increments_from_signup=b.increments_from_signup;

-----------net revenue per trip day of activation trip-----------
DROP TABLE IF EXISTS #net_revenue_of_activation_trip;
SELECT a.channels,
       a.signup_month,
       SUM(b.gaap_net_revenue::float)                         AS gaap_net_revenue,
       SUM(b.paid_days::float)                                AS paid_days,
       SUM(b.gaap_net_revenue::float)/SUM(b.paid_days::float) AS net_revenue_per_day
INTO #net_revenue_of_activation_trip
FROM
    (SELECT driver_id,
            channels,
            signup_month
    FROM #ltr
    GROUP BY 1,2,3) a
LEFT JOIN
    (SELECT * FROM
        (SELECT rs.driver_id,
                rs.reservation_id,
                d.gaap_net_revenue,
                e.paid_days,
                -- Take first trip for a driver
                ROW_NUMBER() OVER (PARTITION BY rs.driver_id ORDER BY rs.trip_start_ts) AS rank1
         FROM analytics.reservation_summary rs
         INNER JOIN (SELECT DISTINCT driver_id FROM #ltr) b
                ON rs.driver_id=b.driver_id
         LEFT JOIN finance.reservation_profit_summary_staging d
                ON rs.reservation_id=d.reservation_id
         LEFT JOIN analytics.reservation_dimensions e
                ON rs.reservation_id=e.reservation_id
         WHERE 1=1
                AND rs.is_ever_booked=1 AND rs.current_status NOT IN (2,11)
                AND rs.trip_start_ts IS NOT null)
    WHERE rank1=1) b -- Net revenue per day from the first trip
ON a.driver_id=b.driver_id
GROUP BY 1, 2
ORDER BY 1, 2;

------------paid days per signup----------------
DROP TABLE IF EXISTS #marketing_paid_days_per_signup_ds;
SELECT a.channels,
       a.signup_month,
       a.increments_from_signup,
       a.activations,
       a.trip_month::date,
       a.ltr_per_signup,
       b.net_revenue_per_day,
       a.ltr_per_signup::float/b.net_revenue_per_day::float AS paid_days_per_signup
INTO #marketing_paid_days_per_signup_ds
FROM #ds_final a
LEFT JOIN #net_revenue_of_activation_trip b
ON a.channels=b.channels and a.signup_month=b.signup_month
ORDER BY 1,2,3;

------------------------------------------------------------------------------------------------------------------------
--paid days curve
------------------------------------------------------------------------------------------------------------------------
-------------------ds curve------------------------
DROP TABLE IF EXISTS #ds_curve_by_signup_month;
WITH pcp_denom AS (
    SELECT *,
           LAG(paid_days_per_signup) OVER (PARTITION BY channels, signup_month ORDER BY increments_from_signup) AS previous_paid_days_per_signup
    FROM #marketing_paid_days_per_signup_ds
)

select * INTO #ds_curve_by_signup_month from
(SELECT channels,
       signup_month,
       increments_from_signup,
       paid_days_per_signup,
       previous_paid_days_per_signup,
       CASE WHEN increments_from_signup=1 THEN 1 ELSE paid_days_per_signup::FLOAT /previous_paid_days_per_signup::FLOAT END AS ds_curve
FROM pcp_denom)
union all
(SELECT channels,
        '2023-04-01' as signup_month,
       increments_from_signup,
       paid_days_per_signup,
       previous_paid_days_per_signup,
       CASE WHEN increments_from_signup=1 THEN 1 ELSE paid_days_per_signup::FLOAT /previous_paid_days_per_signup::FLOAT END AS ds_curve
FROM pcp_denom where signup_month='2023-05-01') --copy May curve to April since April does not prediction 
;


-------------------historical curve------------------------
DROP TABLE IF EXISTS #historical_curve_by_signup_month;
WITH pcp_denom AS (
    SELECT *,
           LAG(paid_days) OVER (PARTITION BY channels, signup_month ORDER BY increments_from_signup) AS previous_paid_days
    FROM #marketing_cohort_metrics
)

SELECT channels,
       signup_month,
       increments_from_signup,
       paid_days,
       previous_paid_days,
       CASE WHEN previous_paid_days=0 THEN null
            WHEN increments_from_signup=1 THEN 1
            ELSE paid_days::FLOAT /previous_paid_days::FLOAT
        END AS historical_curve
INTO #historical_curve_by_signup_month
FROM pcp_denom
;

-- This confuses me, we're taking a single historical curve that's averaged across all months?
DROP TABLE IF EXISTS #avg_historical_curve;
SELECT channels
     , increments_from_signup
     , AVG(historical_curve) AS avg_historical_curve
INTO #avg_historical_curve
FROM #historical_curve_by_signup_month
WHERE TRUE
  AND signup_month>='2016-01-01'
--AND signup_month BETWEEN DATEADD(month, -13, DATE_TRUNC('month', CURRENT_DATE))::D AND DATEADD(month, -2, DATE_TRUNC('month', CURRENT_DATE))::D
GROUP BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------
--paid days curve master
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #demand_paid_days;
SELECT a.*
     , b.ds_curve
     , c.avg_historical_curve
     , a.paid_days*1 AS projected_paid_days -- Why are we multiplying by 1? Value will be null for increments not yet observed
INTO #demand_paid_days
FROM #marketing_cohort_metrics a
         LEFT JOIN (SELECT * FROM #ds_curve_by_signup_month
                    WHERE increments_from_signup<=18) b
              ON a.channels = b.channels
              AND a.signup_month = b.signup_month
              AND a.increments_from_signup = b.increments_from_signup
        -- Shouldn't we use the curve specific to a given signup_month-channel? Why would we expect the current curve to conform to the curve in 2016?
         LEFT JOIN #avg_historical_curve c
              ON a.channels = c.channels
              AND a.increments_from_signup=c.increments_from_signup
;

------------------------------------------------------------------------------------------------------------------------
--net revenue per day curve master
------------------------------------------------------------------------------------------------------------------------
drop table if exists #combine_weight_raw;
select a.signup_month,
       a.channels,
       a.increments_from_signup,
       a.projected_paid_days_per_signup,
       b.signups,
       a.projected_paid_days_per_signup*b.signups as combine
    into #combine_weight_raw
    -- Why is proj paid days per signup coming from a scratch table?
    from analytics_scratch.nrpd_by_channels a
    join (SELECT signup_month,
                 channels,
                 COUNT(DISTINCT driver_id) AS signups
            FROM #signups
            GROUP BY 1,2) b
on a.signup_month=b.signup_month
and a.channels=b.channels
;

DROP TABLE IF EXISTS #demand_nrpd_raw;
select  a.channels,
        a.signup_month,
        b.increments_from_signup,
        b.net_revenue_per_paid_day
into #demand_nrpd_raw
    from #combine_weight_raw a
    left join (select * from analytics_scratch.nrpd_by_channels
                        where case when date_part(day,current_date)<=2 
                                   then signup_month=dateadd('month',-1,date_trunc('month',current_date))
                              else signup_month=dateadd('month',0,date_trunc('month',current_date)) end  --no cps target for current month in the first 2 days
                              -- Will need to clarify this, why are we taking only the latest value? Idea here being that we're going to use the current value for perpetuity going forward? *BE SURE TO CALL OUT*
               ) b
on a.increments_from_signup=b.increments_from_signup
    and a.channels=b.channels
where b.increments_from_signup is not null
;


DROP TABLE IF EXISTS #demand_nrpd;
SELECT a.*
     --, a.net_revenue_per_day*1 AS projected_net_revenue_per_day
     -- If observed, take actual NRPD
        -- If increment in next 12 months, take increment
        -- If increment > 12 months, take value for 12th increment
     , CASE WHEN a.net_revenue_per_day>0 THEN a.net_revenue_per_day
       ELSE (CASE WHEN c.increments_from_signup<=12 THEN c.net_revenue_per_paid_day
             ELSE d.net_revenue_per_paid_day END)
       END AS projected_net_revenue_per_day
INTO #demand_nrpd
FROM (SELECT *,
             CASE WHEN paid_days=0 THEN 0 ELSE net_revenue_usd::float/paid_days::float END AS net_revenue_per_day
      FROM #marketing_cohort_metrics) a
     left join #demand_nrpd_raw c
          on a.channels = c.channels
          and a.signup_month=c.signup_month
          and a.increments_from_signup=c.increments_from_signup
     left join (select * from #demand_nrpd_raw where increments_from_signup=12) d
          on a.channels = d.channels
          and a.signup_month=d.signup_month
;

------------------------------------------------------------------------------------------------------------------------
--------------monaco distribution----------------
DROP TABLE IF EXISTS #temp_driver_monaco;
SELECT a.driver_id,
       a.channels,
       a.signup_month,
       rs.reservation_id,
       rd.paid_days,
       date_trunc('month',rs.trip_end_ts)::D AS trip_month,
       rs.trip_end_ts,
        CASE WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco < 0.01 THEN 'A1'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco <= 0.0045 THEN 'A1'
             WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco < 0.02 THEN 'A2'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco <= 0.009 THEN 'A2'
             WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco < 0.03 THEN 'A3'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco <= 0.0135 THEN 'A3'  
             WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco < 0.06 THEN 'B'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco <= 0.0315 THEN 'B'
             WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco < 0.09 THEN 'C'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco <= 0.05 THEN 'C'
             WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco < 0.12 THEN 'D'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco <= 0.07 THEN 'D'
             WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco < 0.18 THEN 'E'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco <= 0.10 THEN 'E'
             WHEN rd.created::TIMESTAMP < '2023-04-19 18:28:00' AND rd.monaco >= 0.18 THEN 'F'
             WHEN rd.created::TIMESTAMP >= '2023-04-19 18:28:00' AND rd.monaco > 0.10 THEN 'F'
             WHEN rd.monaco IS NULL THEN 'NA'
            ELSE 'NA' END as monaco_bin
INTO #temp_driver_monaco
FROM analytics.reservation_summary rs
INNER JOIN analytics.reservation_dimensions rd
    ON rd.reservation_id = rs.reservation_id
INNER JOIN #signups a
    ON rs.driver_id=a.driver_id
WHERE rs.current_status NOT IN (2,11) AND rs.is_ever_booked=1
    AND date_trunc('month',rs.trip_end_ts)<=dateadd('month',-1,date_trunc('month',CURRENT_DATE))
;

DROP TABLE IF EXISTS #distribution_final;
SELECT a.channels,
       a.trip_month,
       a.monaco_bin,
       a.paid_days,
       b.paid_days_total,
       a.paid_days::float/b.paid_days_total::float AS distribution
INTO #distribution_final
FROM
(SELECT channels,
        trip_month,
        monaco_bin,
        sum(paid_days) AS paid_days
FROM #temp_driver_monaco
GROUP BY 1,2,3) a
LEFT JOIN
(SELECT channels,
        trip_month,
        sum(paid_days) AS paid_days_total
FROM #temp_driver_monaco
GROUP BY 1,2) b
ON a.channels = b.channels
and a.trip_month=b.trip_month
;

------------------#cost_per_trip_day_final-------------------
drop table if exists #cost_per_trip_day_breakdown;
select a.channels,
       a.trip_month as signup_month, --We assume the cost for any signup month is a function of the observed costs for trips taken in that month for ALL users from a given channel. This seems odd and out of step with CPS (e.g. predicting distribution based on past 3 months + adjust for seasonal cost). Confirm undersatnding, no need to solve (we can call out to Andrew)
       a.monaco_bin,
       a.distribution,
       b.total_cost_per_trip_day,
       a.distribution::float * b.total_cost_per_trip_day::float as cost_per_trip_day
into #cost_per_trip_day_breakdown
from
    #distribution_final a
    join
    (select monaco_bin,
            total_cost_per_trip_day
    from marketing_scratch.cost_per_trip_day_by_monaco_demand) b
    on a.monaco_bin=b.monaco_bin;

drop table if exists #cost_per_trip_day_final;
select channels,
       signup_month,
       sum(cost_per_trip_day) as cost_per_trip_day
into #cost_per_trip_day_final
from #cost_per_trip_day_breakdown
group by 1, 2;

------------------------------------------------------------------------------------------------------------------------
--calculate projected pcp
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #avg_halo;
-- Should be redundant since we have a single value for each channel
SELECT channels,
       signup_month,
       AVG(paid_halo) AS avg_paid_halo 
INTO #avg_halo
FROM #signups
GROUP BY 1, 2;

DROP TABLE IF EXISTS #demand_master;
SELECT a.channels,
       a.signup_month,
       a.increments_from_signup,
       a.signups,
       a.paid_days,
       a.trips,
       a.net_revenue_usd                                                                                          AS net_revenue,
       a.actual_partial_contribution_profit_usd                                                                   AS pcp,
       a.partial_contribution_profit_usd                                                                          AS pcp_modified_with_halo,
       a.ds_curve,
       a.avg_historical_curve,
       a.projected_paid_days,   -- Need to be filled for projected values
       d.net_revenue_per_day,
       d.projected_net_revenue_per_day,
       e.cost_per_trip_day,
       c.avg_paid_halo,
       NVL(CASE WHEN a.net_revenue_usd>0 then (a.net_revenue_usd*0.98-a.partial_contribution_profit_usd/avg_paid_halo)/a.paid_days end, e.cost_per_trip_day)  AS projected_cost_per_day
INTO #demand_master
FROM #demand_paid_days a
LEFT JOIN #demand_nrpd d
ON a.channels = d.channels
and a.signup_month=d.signup_month
AND a.increments_from_signup=d.increments_from_signup
LEFT JOIN #avg_halo c --Avg halo is a bit odd since this should be same on channel basis
ON a.channels = c.channels
and a.signup_month=c.signup_month
LEFT JOIN #cost_per_trip_day_final e
ON a.channels = e.channels
and a.signup_month=e.signup_month
;

SELECT *
from #demand_master
where channels in ('Apple','Google_Desktop','Google_Desktop_Brand','Google_Discovery',
    'Google_Mobile','Google_Mobile_Brand','Google_UAC_Android','Kayak_Desktop',
    'Kayak_Desktop_Core', 'Kayak_Mobile_Core','Mediaalpha','Expedia','Microsoft_Desktop',
    'Microsoft_Desktop_Brand','Microsoft_Mobile', 'Microsoft_Mobile_Brand','Kayak_Desktop_Front_Door',
    'Kayak_Desktop_Compare','Google_Pmax','Kayak_Desktop_Carousel','Kayak_Mobile_Carousel',
    'Kayak_Mobile','Kayak_Afterclick', 'Facebook/IG_App', 'Facebook/IG_Web');