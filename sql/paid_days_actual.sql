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

DROP TABLE IF EXISTS #temp_guest_activation;
SELECT DISTINCT a.driver_id
     , FIRST_VALUE(reservation_id)  OVER (PARTITION BY a.driver_id ORDER BY created       ROWS UNBOUNDED PRECEDING)       AS first_booking_reservation_id
     , FIRST_VALUE(reservation_id)  OVER (PARTITION BY a.driver_id ORDER BY trip_start_ts ROWS UNBOUNDED PRECEDING)       AS activation_reservation_id
     , FIRST_VALUE(created)         OVER (PARTITION BY a.driver_id ORDER BY created       ROWS UNBOUNDED PRECEDING)::D    AS first_booking_date
     , FIRST_VALUE(trip_start_ts)   OVER (PARTITION BY a.driver_id ORDER BY trip_start_ts ROWS UNBOUNDED PRECEDING)::D    AS activation_date
INTO #temp_guest_activation
FROM analytics.reservation_summary a
inner join #temp_signup_base b
on a.driver_id=b.driver_id
WHERE 1 = 1
  AND is_ever_booked IN (1)
  AND current_status NOT IN (2, 11)
  and trip_start_ts IS NOT NULL
;

DROP TABLE IF EXISTS #temp_guest_base;
SELECT *
INTO #temp_guest_base
FROM 
    (select sb.driver_id
        , sb.signup_date
        , sb.signup_month
        , sb.channels
        , case when a.platform in ('Android native','Desktop web','iOS native','Mobile web') then a.platform
           else 'Undefined' end as platform_t
        , case when sb.channels in ('Kayak_Desktop', 'Kayak_Desktop_Carousel', 'Kayak_Afterclick', 'Kayak_Desktop_Compare', 'Kayak_Desktop_Front_Door') then 'Kayak_Desktop_Ad'
            when sb.channels in ('Kayak_Mobile_Carousel', 'Kayak_Mobile', 'Kayak_Mobile_Front_Door') then 'Kayak_Mobile_Ad'
            when sb.channels in ('Mediaalpha','Expedia', 'Kayak_Desktop', 'Kayak_Desktop_Carousel', 'Kayak_Afterclick', 'Kayak_Desktop_Compare', 
                'Kayak_Desktop_Front_Door', 'Kayak_Mobile_Carousel', 'Kayak_Mobile', 'Kayak_Mobile_Front_Door',
                'Kayak_Desktop_Core', 'Kayak_Mobile_Core') then 'all_travel_agency'
            when sb.channels in ('Facebook/IG_Web', 'Facebook/IG_App', 'Facebook_Free', 'Reddit') then 'all_social_media'
            else sb.channels end as segment
        , sb.platform
        , sb.country
        , ga.first_booking_date::D                        AS first_booking_date
        , DATE_TRUNC('month', ga.first_booking_date)::D   AS first_booking_month
        , ga.activation_date::D                           AS activation_date
        , DATE_TRUNC('month', ga.activation_date)::D      AS activation_month
    from #temp_signup_base sb
    LEFT JOIN #temp_guest_activation ga
        ON sb.driver_id = ga.driver_id
    LEFT JOIN (select driver_id,max(platform) as platform from marketing.marketing_channel_by_driver group by driver_id) a
        ON sb.driver_id = a.driver_id
    -- filter to drivers from whom we observe two full increments
    where DATEADD('day', 60, sb.signup_date)::Date < CURRENT_DATE - 1)
union all
    (select sb.driver_id
        , sb.signup_date
        , sb.signup_month
        , sb.channels
        , case when a.platform in ('Android native','Desktop web','iOS native','Mobile web') then a.platform
           else 'Undefined' end as platform_t
        , 'all_app' as segment
        , sb.platform
        , sb.country
        , ga.first_booking_date::D                        AS first_booking_date
        , DATE_TRUNC('month', ga.first_booking_date)::D   AS first_booking_month
        , ga.activation_date::D                           AS activation_date
        , DATE_TRUNC('month', ga.activation_date)::D      AS activation_month
    from #temp_signup_base sb
    LEFT JOIN #temp_guest_activation ga
        ON sb.driver_id = ga.driver_id
    LEFT JOIN (select driver_id,max(platform) as platform from marketing.marketing_channel_by_driver group by driver_id) a
        ON sb.driver_id = a.driver_id
    -- filter to drivers from whom we observe two full increments
    where DATEADD('day', 60, sb.signup_date)::Date < CURRENT_DATE - 1
        and platform_t in ('Android native','iOS native'))
union all
    (select sb.driver_id
        , sb.signup_date
        , sb.signup_month
        , sb.channels
        , case when a.platform in ('Android native','Desktop web','iOS native','Mobile web') then a.platform
           else 'Undefined' end as platform_t
        , sb.channels as segment
        , sb.platform
        , sb.country
        , ga.first_booking_date::D                        AS first_booking_date
        , DATE_TRUNC('month', ga.first_booking_date)::D   AS first_booking_month
        , ga.activation_date::D                           AS activation_date
        , DATE_TRUNC('month', ga.activation_date)::D      AS activation_month
    from #temp_signup_base sb
    LEFT JOIN #temp_guest_activation ga
        ON sb.driver_id = ga.driver_id
    LEFT JOIN (select driver_id,max(platform) as platform from marketing.marketing_channel_by_driver group by driver_id) a
        ON sb.driver_id = a.driver_id
    -- filter to drivers from whom we observe two full increments
    where DATEADD('day', 60, sb.signup_date)::Date < CURRENT_DATE - 1
        and sb.channels in ('Kayak_Desktop_Core', 'Kayak_Mobile_Core'))
;

DROP TABLE IF EXISTS #temp_guest_group_map;
SELECT *
     , DENSE_RANK() OVER (ORDER BY
       country
     , segment
     , signup_month
     ) AS group_id
INTO #temp_guest_group_map
FROM #temp_guest_base
;

DROP TABLE IF EXISTS #temp_guest_group;
SELECT a.group_id
     , a.country
     , a.segment
     , a.signup_month
     , b.signups
INTO #temp_guest_group
FROM #temp_guest_group_map a
left join
    (select country,segment,signup_month,count(distinct driver_id) as signups
        from #temp_guest_group_map
        group by 1,2,3) b
on a.country=b.country and a.segment=b.segment and a.signup_month=b.signup_month
GROUP BY 1, 2, 3, 4, 5
;

DROP TABLE IF EXISTS #temp_guest_group_add_activations;
SELECT group_id
     , country
     , segment
     , signup_month
     , FLOOR(DATEDIFF(day, signup_date, activation_date)/30) + 1 AS increments_from_signup
     , count(distinct driver_id) as activations
INTO #temp_guest_group_add_activations
FROM #temp_guest_group_map
where activation_date is not null
group by 1,2,3,4,5;

DROP TABLE IF EXISTS #temp_guest_revenue_summary;
select t1.*,
       t2.activations,
       sum(t2.activations) over (partition by t2.group_id order by t2.increments_from_signup ROWS UNBOUNDED PRECEDING) as accumulated_activations
INTO #temp_guest_revenue_summary
from
    (SELECT gm.group_id
         , FLOOR(DATEDIFF(day, gm.signup_date, grb.date::date)/30) + 1 AS increments_from_signup
         , SUM(grb.gross_revenue_usd)  AS gross_revenue_usd
         , SUM(CASE WHEN grb.revenue_category IN ('rental_revenue', 'rental_revenue_discount', 'rental_revenue_boost') THEN grb.gross_revenue_usd ELSE 0 END)                       AS trip_revenue_usd
         , SUM(grb.host_payment_usd)                                                                                                                                                AS host_earnings_usd
         , SUM(grb.net_revenue_usd)                                                                                                                                                 AS net_revenue_usd
         , SUM(CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) AND grb.revenue_category = 'rental_revenue' THEN booked_noc_days ELSE 0 END)                    AS booked_noc_days
         , SUM(CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) AND grb.revenue_category = 'rental_revenue' THEN paid_days ELSE 0 END)                          AS paid_days
         , SUM(CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) AND grb.revenue_category = 'rental_revenue' THEN gaap_trip_count ELSE 0 END)                    AS trips
         , COUNT(DISTINCT CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) AND grb.revenue_category = 'rental_revenue' THEN grb.reservation_id || grb.date END) AS trip_days
         , COUNT(DISTINCT CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) AND grb.revenue_category = 'rental_revenue' THEN rs.driver_id END)                   AS active_guests

    FROM finance.gaap_revenue_breakdown grb
             JOIN analytics.reservation_summary rs
                  ON rs.reservation_id = grb.reservation_id
             JOIN #temp_guest_group_map gm
                  ON rs.driver_id = gm.driver_id
    WHERE 1 = 1
      and grb.date>=gm.activation_date
      AND grb.date < CURRENT_DATE - 1
      AND DATEADD('day', 60, gm.signup_date)::Date < CURRENT_DATE - 1
    GROUP BY 1, 2) t1
left join #temp_guest_group_add_activations t2
on t1.group_id=t2.group_id and t1.increments_from_signup=t2.increments_from_signup
;


DROP TABLE IF EXISTS #temp_guest_cost_summary;
SELECT gm.group_id
     , FLOOR(DATEDIFF(day, gm.signup_date, d.date::date)/30) + 1 AS increments_from_signup
     , SUM(NVL(ps.protection_total      , 0) / rd.paid_days::FLOAT)     AS protection_cost
     , SUM(NVL(ps.liability_total       , 0) / rd.paid_days::FLOAT)     AS cost_liability
     , SUM(NVL(ps.customer_support      , 0) / rd.paid_days::FLOAT)     AS customer_support_cost
     , SUM(NVL(ps.payment_processing    , 0) / rd.paid_days::FLOAT)     AS payment_processing_cost
     , SUM(NVL(ps.incidental_bad_debt   , 0) / rd.paid_days::FLOAT)     AS incidental_bad_debt
     , SUM(NVL(ps.chargeback_plus_fee   , 0) / rd.paid_days::FLOAT)     AS chargebacks
     , SUM(NVL(ps.valet                 , 0) / rd.paid_days::FLOAT)     AS valet
     , SUM((NVL(ps.protection_total     , 0)
         + NVL(ps.liability_total       , 0)
         + NVL(ps.customer_support      , 0)
         + NVL(ps.payment_processing    , 0)
         + NVL(ps.incidental_bad_debt   , 0)
         + NVL(ps.chargeback_plus_fee   , 0)
         + NVL(ps.valet                 , 0)) / rd.paid_days::FLOAT)    AS total_cost
INTO #temp_guest_cost_summary
FROM finance.reservation_profit_summary ps -- how many months
         JOIN analytics.reservation_summary rs
              ON ps.reservation_id = rs.reservation_id
         JOIN analytics.reservation_dimensions rd
              ON rs.reservation_id = rd.reservation_id
         JOIN #temp_guest_group_map gm
              ON rs.driver_id = gm.driver_id
         JOIN analytics.date d
              ON d.date BETWEEN COALESCE(CASE WHEN rs.current_status IN (2, 11) THEN rs.modified::D END, rs.current_start_ts::D)
                            AND COALESCE(CASE WHEN rs.current_status IN (2, 11) THEN rs.modified::D END, DATEADD('day', ABS(rd.paid_days)::INT - 1, rs.current_start_ts::D))
WHERE 1 = 1
  and d.date>=gm.activation_date
  AND COALESCE(CASE WHEN rs.current_status IN (2, 11) THEN rs.modified::DATE END, d.date) < CURRENT_DATE - 1
  AND DATEADD('day', 60, gm.signup_date)::Date < CURRENT_DATE - 1
GROUP BY 1, 2
;

drop table if exists #temp_search_session;
select gm.group_id
     , FLOOR(DATEDIFF(day, gm.signup_date, sd.created::date)/30) + 1 AS increments_from_signup
     , count(sd.session_id) as search_sessions
into #temp_search_session
from analytics.session_dimensions sd
         JOIN #temp_guest_group_map gm
              ON sd.driver_id = gm.driver_id
WHERE 1 = 1
  and sd.search=1
  and sd.created>=gm.signup_date
  AND sd.created < CURRENT_DATE - 1
  AND DATEADD('day', 60, gm.signup_date)::Date < CURRENT_DATE - 1
group by 1,2
;

DROP TABLE IF EXISTS #guest_cohort_summary;
SELECT DENSE_RANK() OVER (ORDER BY gg.country, gg.segment, gg.signup_month) AS cohort_id
     , gg.group_id
     , gg.country
     , gg.segment
     , gg.signup_month
     , gg.signups
     , grs.increments_from_signup
     , NVL(gross_revenue_usd, 0)                        AS gross_revenue_usd
     , NVL(trip_revenue_usd, 0)                         AS trip_revenue_usd
     , NVL(host_earnings_usd, 0)                        AS host_earnings_usd
     , NVL(net_revenue_usd, 0)                          AS net_revenue_usd
     , NVL(booked_noc_days, 0)                          AS booked_noc_days
     , NVL(paid_days, 0)                                AS paid_days
     , NVL(trips, 0)                                    AS trips
     , NVL(trip_days, 0)                                AS trip_days
     , NVL(active_guests, 0)                            AS active_guests
     , NVL(activations,0)                               AS activations
     , NVL(accumulated_activations,0)                   AS accumulated_activations
     , NVL(total_cost, 0)                               AS total_cost
     , NVL(net_revenue_usd, 0) - NVL(total_cost, 0)     AS adjusted_contribution
     , NVL(ss.search_sessions,0)                        AS search_sessions
INTO #guest_cohort_summary
FROM #temp_guest_group gg
         LEFT JOIN #temp_guest_revenue_summary grs
                   ON gg.group_id = grs.group_id
         LEFT JOIN #temp_guest_cost_summary gcs
                   ON grs.group_id = gcs.group_id
                       and grs.increments_from_signup=gcs.increments_from_signup
         left join #temp_search_session ss
                   on grs.group_id=ss.group_id
                       and grs.increments_from_signup=ss.increments_from_signup
;

------------------------------------------------------build #marketing_cost--------------------------------------------------------------
drop table if exists #marketing_cost;
select country,
       signup_month,
       channel_lvl_5 as channels,
       sum(calculated_marketing_cost) as marketing_cost
into #marketing_cost
from marketing.marketing_cps_cpa_by_channels
group by 1,2,3
;


drop table if exists #historical_paid_days_per_signup;
select t1.*
into #historical_paid_days_per_signup
from
(select cohort_id,
       country,
       segment,
       signup_month,
       increments_from_signup,
       signups,
       sum(paid_days) as paid_days,
       sum(net_revenue_usd) as net_revenue,
       sum(adjusted_contribution) as adjusted_contribution,
       sum(total_cost) as cost,
       sum(trips) as trips,
       sum(activations) as activations,
       sum(accumulated_activations) as accumulated_activations,
       sum(search_sessions) as search_sessions
from #guest_cohort_summary a
where 1=1
--and increments_from_signup<=12
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6) t1
;

-- actual pdps for segment
DROP TABLE IF EXISTS #actual_pdps_segment;
select *,
       paid_days_per_signup_original*seasonal_index as projected_paid_days
into #actual_pdps_segment
from (select segment,
           signup_month,
           increments_from_signup,
           signups::float as num_signups_actual_by_increment,
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
         from #historical_paid_days_per_signup
        -- Take the actuals corresponding to 3 months ago to predict for a given signup month
         where country='US' and increments_from_signup<=12);


select a.channels,
    b.*
from (
    select channels,
        case when channels in ('Kayak_Desktop', 'Kayak_Desktop_Carousel', 'Kayak_Afterclick', 'Kayak_Desktop_Compare', 'Kayak_Desktop_Front_Door') then 'Kayak_Desktop_Ad'
            when channels in ('Kayak_Mobile_Carousel', 'Kayak_Mobile', 'Kayak_Mobile_Front_Door') then 'Kayak_Mobile_Ad'
            when channels in ('Mediaalpha', 'Expedia') then 'all_travel_agency'
            when channels in ('Apple') then 'all_app'
            when channels in ('Facebook/IG_Web', 'Facebook/IG_App', 'Reddit') then 'all_social_media'
            else channels end as segment
    from (select distinct channels from #temp_signup_base)
) a
left join #actual_pdps_segment b
on a.segment = b.segment 
where channels in ('Apple', 'Apple_Brand', 'Google_Desktop','Google_Desktop_Brand',
            'Google_Mobile','Google_Mobile_Brand','Kayak_Desktop', 'Kayak_Desktop_Core', 
            'Kayak_Mobile_Core','Mediaalpha','Expedia','Microsoft_Desktop',
            'Microsoft_Desktop_Brand', 'Reddit', 'Moloco', 'Kayak_Desktop_Compare',
            'Google_Pmax','Kayak_Desktop_Carousel','Kayak_Mobile_Carousel',
            'Kayak_Afterclick', 'Facebook/IG_App', 'Facebook/IG_Web');