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
where country in ('US')
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
    and nvl(sucf.rank_desc_paid_90_days, sucf.rank_desc_90_days) = 1
;

drop table if exists #data_warehouse_signups_pick;
select *,
       1 as pick_rank
into #data_warehouse_signups_pick
from #data_warehouse_signups
where 1=1
    and country = 'US'
    and channels in ('Apple_Brand', 'Facebook/IG_App', 'Facebook/IG_Web', 'Facebook_Free')
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

------------------------------------------------------------------------------------------------------------------------
-- Trips all
------------------------------------------------------------------------------------------------------------------------
drop table if exists #trips_all;
SELECT s.channels
        , case when a.platform in ('Android native','Desktop web','iOS native','Mobile web') then a.platform
           else 'Undefined' end as platform
        , FLOOR(DATEDIFF(day, s.signup_date, d.date::date)/30) + 1                                                           AS increments_from_signup
        , DATEADD('month', CAST(increments_from_signup - 1 AS int), s.signup_month)                                          AS trip_month
        , CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END                             AS paid_day
        , CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END / rd.paid_days::FLOAT       AS trip
        , rps.gaap_net_revenue / rd.paid_days::FLOAT                                                                    AS net_revenue_usd
INTO #trips_all
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
        JOIN #temp_signup_base s
            ON rs.driver_id = s.driver_id
        JOIN analytics.date d
            ON d.date BETWEEN COALESCE(rs.trip_start_ts, rd.current_start_ts)::D AND DATEADD('day', rd.paid_days::INT - 1, COALESCE(rs.trip_start_ts, rs.current_start_ts))::D
        LEFT JOIN (select driver_id,max(platform) as platform from marketing.marketing_channel_by_driver group by driver_id) a
            on rs.driver_id = a.driver_id
WHERE TRUE
    -- drop reservations where trip dates were before their signup date (some errors in the observations)
    AND s.signup_month <= d.date
    AND trip_month < DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE));

------------------------------------------------------------------------------------------------------------------------
-- NRPD actual by segment
------------------------------------------------------------------------------------------------------------------------
drop table if exists #nrpd_actual_by_segment;
select *
into #nrpd_actual_by_segment
from (
    select case when channels in ('Kayak_Desktop', 'Kayak_Desktop_Carousel', 'Kayak_Afterclick', 'Kayak_Desktop_Compare', 'Kayak_Desktop_Front_Door') then 'Kayak_Desktop_Ad' 
                else channels end           AS segment
     , trip_month                                                                                                        AS month
     , increments_from_signup
     , SUM(paid_day)                                                                                                     AS paid_days
     , SUM(net_revenue_usd)                                                                                              AS net_revenue
     -- Track of data volume (i.e. trips) for each channel
     , SUM(trip)                                                                                                         AS data_volume
     , net_revenue/paid_days                                                                                             AS nrpd   
    from #trips_all
    where channels is not null
    group by 1, 2, 3)
UNION all
    (select 'Kayak_Mobile_Ad' AS segment
     , trip_month                                                                                                        AS month
     , increments_from_signup
     , SUM(paid_day)                                                                                                     AS paid_days
     , SUM(net_revenue_usd)                                                                                              AS net_revenue
     -- Track of data volume (i.e. trips) for each channel
     , SUM(trip)                                                                                                         AS data_volume
     , net_revenue/paid_days                                                                                             AS nrpd   
    from #trips_all
    where channels in ('Kayak_Mobile_Carousel', 'Kayak_Mobile', 'Kayak_Mobile_Front_Door')
    group by 1, 2, 3)
UNION all
    (select 'all_travel_agency' AS segment
     , trip_month                                                                                                        AS month
     , increments_from_signup
     , SUM(paid_day)                                                                                                     AS paid_days
     , SUM(net_revenue_usd)                                                                                              AS net_revenue
     -- Track of data volume (i.e. trips) for each channel
     , SUM(trip)                                                                                                         AS data_volume
     , net_revenue/paid_days                                                                                             AS nrpd   
    from #trips_all
    where channels in ('Mediaalpha','Expedia', 'Kayak_Desktop', 'Kayak_Desktop_Carousel', 'Kayak_Afterclick', 'Kayak_Desktop_Compare', 
        'Kayak_Desktop_Front_Door', 'Kayak_Mobile_Carousel', 'Kayak_Mobile', 'Kayak_Mobile_Front_Door'
        'Kayak_Desktop_Core', 'Kayak_Mobile_Core')
    group by 1, 2, 3)
UNION all
    (select 'all_paid' AS segment
     , trip_month                                                                                                        AS month
     , increments_from_signup
     , SUM(paid_day)                                                                                                     AS paid_days
     , SUM(net_revenue_usd)                                                                                              AS net_revenue
     -- Track of data volume (i.e. trips) for each channel
     , SUM(trip)                                                                                                         AS data_volume
     , net_revenue/paid_days                                                                                             AS nrpd   
    from #trips_all
    where channels is not null
        and channels <> 'Facebook_Free'
    group by 1, 2, 3)
UNION all
    (select 'all_app' AS segment
     , trip_month                                                                                                        AS month
     , increments_from_signup
     , SUM(paid_day)                                                                                                     AS paid_days
     , SUM(net_revenue_usd)                                                                                              AS net_revenue
     -- Track of data volume (i.e. trips) for each channel
     , SUM(trip)                                                                                                         AS data_volume
     , net_revenue/paid_days                                                                                             AS nrpd   
    from #trips_all
    where platform in ('Android native','iOS native')
    group by 1, 2, 3)
UNION all
    (select 'all_social_media' AS segment
     , trip_month                                                                                                        AS month
     , increments_from_signup
     , SUM(paid_day)                                                                                                     AS paid_days
     , SUM(net_revenue_usd)                                                                                              AS net_revenue
     -- Track of data volume (i.e. trips) for each channel
     , SUM(trip)                                                                                                         AS data_volume
     , net_revenue/paid_days                                                                                             AS nrpd   
    from #trips_all
    where channels in ('Facebook/IG_Web', 'Facebook/IG_App', 'Facebook_Free', 'Reddit')
    group by 1, 2, 3);


------------------------------------------------------------------------------------------------------------------------
-- NRPD actual by channel
------------------------------------------------------------------------------------------------------------------------
SELECT a.channels,
    b.*                                                                                          
FROM (SELECT channels,
        -- mapping table from channel to segment
        case when channels in ('Kayak_Desktop', 'Kayak_Desktop_Carousel', 'Kayak_Afterclick', 'Kayak_Desktop_Compare', 'Kayak_Desktop_Front_Door') then 'Kayak_Desktop_Ad'
            when channels in ('Kayak_Mobile_Carousel', 'Kayak_Mobile', 'Kayak_Mobile_Front_Door') then 'Kayak_Mobile_Ad'
            when channels in ('Mediaalpha', 'Expedia') then 'all_travel_agency'
            when channels in ('Apple') then 'all_app'
            when channels in ('Facebook/IG_Web', 'Facebook/IG_App', 'Reddit') then 'all_social_media'
            else channels end as segment
    FROM 
    (SELECT distinct channels from #trips_all)) as a
LEFT JOIN #nrpd_actual_by_segment as b on a.segment = b.segment
where a.channels in ('Apple', 'Apple_Brand', 'Google_Desktop','Google_Desktop_Brand',
            'Google_Mobile','Google_Mobile_Brand','Kayak_Desktop', 'Kayak_Desktop_Core', 
            'Kayak_Mobile_Core','Mediaalpha','Expedia','Microsoft_Desktop',
            'Microsoft_Desktop_Brand', 'Reddit', 'Moloco', 'Kayak_Desktop_Compare',
            'Google_Pmax','Kayak_Desktop_Carousel','Kayak_Mobile_Carousel',
            'Kayak_Afterclick', 'Facebook/IG_App', 'Facebook/IG_Web');