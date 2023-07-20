CREATE TEMP FUNCTION
  get_event(
    key_ STRING, 
    arr ARRAY<STRUCT<key STRING, 
    value STRUCT<
string_value STRING, 
int_value INT64, 
float_value FLOAT64,
double_value FLOAT64>>>) AS ( 
(
    SELECT       
        data.value     
    FROM       
        UNNEST(arr) as data
    WHERE       
        data.key = key_ 
) 
);

CREATE TEMP FUNCTION
  get_user(
    key_ STRING, 
    arr ARRAY<STRUCT<key STRING, 
    value STRUCT<
string_value STRING, 
int_value INT64, 
float_value FLOAT64,
double_value FLOAT64,
set_timestamp_micros INT64>>>) AS ( 
(
    SELECT       
        data.value     
    FROM       
        UNNEST(arr) as data
    WHERE       
        data.key = key_ 
) 
);



CREATE TEMP FUNCTION
create_channel(medium string,
    name string,
    source string) as
    (
    (
       CASE 
    
        WHEN source = '(direct)' AND (medium IN ('(not set)','(none)')) THEN 'Direct'
            
        WHEN regexp_contains(name, 'cross-network') THEN 'Cross-network'
    
        WHEN (regexp_contains(source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
            OR regexp_contains(name, '^(.*(([^a-df-z]|^)shop|shopping).*)$'))
            AND regexp_contains(medium, '^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Shopping'
    
        WHEN regexp_contains(source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
            AND regexp_contains(medium,'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Search'
    
        WHEN regexp_contains(source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
            AND regexp_contains(medium,'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Social'
    
        WHEN regexp_contains(source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
            AND regexp_contains(medium,'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Video'
    
        WHEN medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
    
        WHEN regexp_contains(medium,'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Other'
    
        WHEN regexp_contains(source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
            OR regexp_contains(name, '^(.*(([^a-df-z]|^)shop|shopping).*)$') THEN 'Organic Shopping'
    
        WHEN regexp_contains(source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
            OR medium IN ('social','social-network','social-media','sm','social network','social media') THEN 'Organic Social'
    
        WHEN regexp_contains(source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
            OR regexp_contains(medium,'^(.*video.*)$') THEN 'Organic Video'
    
        WHEN regexp_contains(source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
            OR medium = 'organic' THEN 'Organic Search'
    
        WHEN (medium IN  ("referral", "app", "link")) THEN 'Referral'
    
        WHEN regexp_contains(source,'email|e-mail|e_mail|e mail')
            OR regexp_contains(medium,'email|e-mail|e_mail|e mail') THEN 'Email'
    
        WHEN medium = 'affiliate' THEN 'Affiliates'
    
        WHEN medium = 'audio' THEN 'Audio'
    
        WHEN medium = 'sms' OR source = 'sms'  THEN 'SMS'
    
        WHEN medium LIKE '%push'
            OR regexp_contains(medium,'mobile|notification')
            OR source='firebase' THEN 'Mobile Push Notifications'
    
        ELSE 'Unassigned' END
    )
    );

CREATE TEMP FUNCTION
channel_grouping(channel string) as
(
(
    CASE 
        WHEN LOWER(channel) IN ('direct', 'organic search', 'referral', 'social', 'paid search', 'email', 'display') THEN channel
    ELSE '(Other)' END
)
);


WITH 
get_data AS (
SELECT 
    user_pseudo_id AS fullVisitorId,
    CONCAT(user_pseudo_id, get_event('ga_session_id', ev.event_params).int_value) AS visitId,  
    PARSE_DATE("%Y%m%d", event_date) AS date_5,
    event_timestamp,
    event_name,
    CONCAT(user_pseudo_id, get_event('ga_session_id', ev.event_params).int_value, event_name, event_timestamp) AS event_id, 
    CASE 
        WHEN LOWER(platform)='web' THEN 'Website' 
        WHEN LOWER(platform) in ('android', 'ios') THEN 'App'
        ELSE NULL END AS platform_id_10,
    ev.collected_traffic_source.manual_campaign_id AS acq_channel_id_12,
    
    channel_grouping(
        create_channel(
            IF(LOWER(ev.collected_traffic_source.manual_source)='google' AND 
                LOWER(ev.collected_traffic_source.manual_medium)='organic' AND 
                ev.collected_traffic_source.gclid IS NOT NULL, 
            'cpc',
            LOWER(ev.collected_traffic_source.manual_medium)),
            LOWER(ev.collected_traffic_source.manual_campaign_name), 
            LOWER(ev.collected_traffic_source.manual_source))) AS channel_name_13,

    device.category AS device_category_24,
    geo.country AS country_name_30,
    get_event('country', ev.event_params).string_value AS country_code_uefa_30, 
    get_user('login_status', ev.user_properties).string_value AS login_status, 

     IF(LOWER(ev.collected_traffic_source.manual_source)='google' AND 
                LOWER(ev.collected_traffic_source.manual_medium)='organic' AND 
                ev.collected_traffic_source.gclid IS NOT NULL, 
            'cpc',
            LOWER(ev.collected_traffic_source.manual_medium)) AS Custom_Acq_Channel_Name,
            
    ev.collected_traffic_source.manual_campaign_name AS Campaign_56,
    ev.collected_traffic_source.manual_source AS Traffic_Source_62,
    geo.city AS City_Name_65,
    NULL AS City_ID_64
    --get_event('engagement_time_msec', ev.event_params).int_value AS engagement_time
FROM 
    `bigquery-for-trakken.analytics_303339971.events_@date` AS ev
),

clean_data AS (
    SELECT *
    FROM get_data
    WHERE fullVisitorId IS NOT NULL AND visitId IS NOT NULL
),

create_session_level_metrics AS(
    -- session_duration
    -- hits
    -- newVisits
    -- pageviews_44
    -- screenviews_44
    -- nbr_loggedin_user_46
    -- total_visit_51
    -- engagement_time
    SELECT
        visitId,
        (MAX(event_timestamp) - MIN(event_timestamp))/1000000 AS session_duration, 
        COUNT(DISTINCT event_id) AS hits, 
        CAST(MAX(IF(event_name IN ('first_visit','first_open'),1,0)) AS BOOLEAN)  AS newVisits,
        COUNT( DISTINCT IF(event_name='page_view', event_id, NULL )) AS pageviews_44,
        COUNT( DISTINCT IF(event_name='screen_view',  event_id, NULL )) AS screenviews_44,
        CAST(MAX(IF(LOWER(login_status) IN ('logged in', 'true'), 1, 0)) AS BOOLEAN) AS nbr_loggedin_user_46,
        MAX(IF(event_name NOT IN ('view_item', 'screenview', 'screen_view', 
            'onboarding_pick_teams_screen_view', 'gdpr_screen_view', 'page_view', 
            'view_item_list', 'view_promotion', 'scroll'), 1, 0)) AS total_visit_51
        --SUM(engagement_time)/1000 AS engagement_time
    FROM 
        clean_data 
    GROUP BY 
        1
),

filter_data_to_session_level AS (
    SELECT 
        session.*EXCEPT(event_timestamp, event_name,visit_start_time, login_status, event_id)
    FROM (
        SELECT ARRAY_AGG(results ORDER BY results.visit_start_time LIMIT 1) [OFFSET(0)]  AS session
        FROM (
            SELECT
                *, 
                MIN(event_timestamp) OVER (PARTITION BY visitId) AS visit_start_time 
            FROM 
                clean_data) AS results
        GROUP BY results.visitId
    )
),

join_session_metrics AS (
    SELECT 
        filter_data_to_session_level.*,
        create_session_level_metrics.*EXCEPT(visitId)
    FROM 
        filter_data_to_session_level
    LEFT JOIN 
        create_session_level_metrics USING(visitId)
),

create_time_on_screen_page AS (
    SELECT
        *,
        IF(platform_id_10='Website', session_duration, NULL) AS timeonesite,
        IF(platform_id_10='App', session_duration, NULL) AS timeOnScreen
    FROM 
        join_session_metrics 
)

SELECT DISTINCT * EXCEPT(session_duration)
FROM create_time_on_screen_page