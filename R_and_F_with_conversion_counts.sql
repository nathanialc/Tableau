CREATE TABLE user_impressions as (
SELECT
  user_id
  ,event.impression_id as impression_id
  ,timestamp_micros(event.event_time) as event_timestamp
  ,adv.advertiser as advertiser
  ,p.site_placement_ID as placement_ID 
FROM
 adh.cm_dt_impressions imp
    LEFT JOIN adh.cm_dt_advertiser adv
    ON imp.event.advertiser_id = adv.advertiser_id
    LEFT JOIN adh.cm_dt_placement p
    ON imp.event.placement_id = p.site_placement_id   
   WHERE
   event.advertiser_id in unnest(@ADVERTISERS)
  AND
    date(timestamp_micros(event.event_time)) between @FOCUS_DATE_START and @FOCUS_DATE_END
GROUP BY 1,2,3,4,5
);



CREATE TABLE user_date_impressions as (
  SELECT
   user_id
   ,date(event_timestamp) as event_date
   ,advertiser  
   ,placement_ID
   ,count(*) as imp_cnt
  FROM tmp.user_impressions
  GROUP BY
   1,2,3,4
); 

CREATE TABLE user_conversions as ( 
  SELECT
   user_id
   ,date(timestamp_micros(event.event_time)) as activity_date
   ,max(case when event.activity_count > 0 then 1 else 0 end) as has_conv
  FROM adh.cm_dt_activities 
  where event.activity_id in unnest(@ACTIVITIES) 
  GROUP BY 1,2
);


CREATE TABLE placement_and_conversions as (
SELECT
 i.user_id
 ,i.event_date
 ,i.advertiser
 ,map.campaign
 --,i.placement
 ,map.region 
 ,map.audience
 ,map.tactics
 ,imp_cnt
 ,max(case when cv.has_conv is null then 0 else 1 end) as has_conv
FROM
 tmp.user_date_impressions as i
 LEFT JOIN `jfgp-jellyfish-adh.tableau.placement_mappings` as map
 	on i.placement_ID = map.placement_id
 LEFT JOIN tmp.user_conversions cv
 	ON i.user_id = cv.user_id
  WHERE (i.event_date >= date_sub(cv.activity_date, interval 30 DAY)
    AND i.event_date <= cv.activity_date)
   OR cv.has_conv is null
GROUP BY
 1,2,3,4,5,6,7,8
);


CREATE TABLE user_imp_cnt as (
  SELECT
    user_id
    ,advertiser
    ,campaign
    ,region 
 		,audience
 		,tactics
    ,round(avg(imp_cnt)) as imp_cnt
    ,max(has_conv) as has_conv
  FROM tmp.placement_and_conversions
  GROUP BY 1,2,3,4,5,6
);

SELECT
 	advertiser
 ,campaign
 --,placement
 ,region 
 ,audience
 ,tactics
 ,imp_cnt
 ,count(distinct user_id) as user_cnt
 ,sum(has_conv) as conv_cnt
FROM 
	tmp.user_imp_cnt 

GROUP BY
	1,2,3,4,5,6