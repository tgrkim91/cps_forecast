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

select *
from #marketing_cohort_metrics
where signup_month >= '2024-01-01'
