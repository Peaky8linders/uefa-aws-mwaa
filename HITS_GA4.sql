
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




WITH
get_data AS (
    SELECT 
        user_pseudo_id AS fullVisitorId, 
        CONCAT(user_pseudo_id, get_event('ga_session_id', ev.event_params).int_value) AS visitId, 
        event_name,
        event_timestamp,
        CONCAT(user_pseudo_id, get_event('ga_session_id', ev.event_params).int_value, event_name, event_timestamp) AS event_id,

        stream_id AS product_id_14,  
        NULL as product_name_displayname_15,

        get_event('link_domain', ev.event_params).string_value AS product_name_hostname_15, 

        get_event('competition', ev.event_params).string_value AS competition_16, 
        --page_name did not have the names there where registered in UA
        get_event('page_path', ev.event_params).string_value AS page_name_17,

        --although not all names showed
        get_event('page_title', ev.event_params).string_value AS page_name_18, 
        -- screen_name (values did not match check)
        get_event('screen_name', ev.event_params).string_value AS screen_name_19, 
        
        --the dimension as previous
        NULL AS Source_Property_Tracking_ID_20,
        NULL AS hostname_21,
        NULL AS Source_Property_Display_Name_22, 

        UPPER(get_event('page_language', ev.event_params).string_value) AS language_26,
        get_event('article_master_id', ev.event_params).string_value AS article_master_id_27, 
        get_event('article_child_id', ev.event_params).string_value AS article_child_id_27, 
        get_event('page_type', ev.event_params).string_value AS page_type_28,

        get_event('event_category', ev.event_params).string_value AS event_category,
        get_event('event_action', ev.event_params).string_value AS event_action,
        IF(device.category='smart tv', device.operating_system, NULL) AS TV_Platforms_55,
        get_event('match_id', ev.event_params).string_value AS Match_ID, 
        get_event('match_name', ev.event_params).string_value AS Match_Name, 
        get_event('event_label', ev.event_params).string_value AS event_label_57, 

        get_event('video_type', ev.event_params).string_value AS videoType_58, 

        CASE 
            WHEN event_name='page_view' THEN 'PAGE'
            WHEN event_name='screen_view' THEN 'APPVIEW'
        ELSE 'EVENT' END AS hit_type_59, 

        get_event('team_id', ev.event_params).string_value AS Team_ID_63, 

        get_event('video_id', ev.event_params).string_value AS video_id_68, 

        get_event('article_master_title', ev.event_params).string_value AS article_master_name_69,

        get_event('article_child_title', ev.event_params).string_value AS articleChildName_69, 

        get_event('video_title', ev.event_params).string_value AS video_name_70, 

        get_user('newsletter_subscriber', ev.user_properties).string_value AS user_news_letter_71, 

        get_user('user_team', ev.user_properties).string_value AS user_team_72,

        IF(event_name='video_start',get_event('video_duration', ev.event_params).int_value, NULL) AS video_length_73,

        IFNULL(get_event('player_id', ev.event_params).string_value,
            CAST(get_event('player_id', ev.event_params).int_value AS string) ) AS player_id_74, 

        get_event('partners', ev.event_params).string_value AS exclusive_promo_id_75,

        ARRAY_TO_STRING(ARRAY(
            SELECT IF(items.promotion_id='(not set)', NULL,items.promotion_id)  
            FROM UNNEST(items) AS items 
            ORDER BY items.promotion_id), 
                ',') AS promoId,

        ARRAY_TO_STRING(ARRAY(
            SELECT IF(items.promotion_name='(not set)', NULL,items.promotion_name)  
            FROM UNNEST(items) AS items 
            ORDER BY items.promotion_name), 
                ',') AS promoName, 

        ARRAY_TO_STRING(ARRAY(
            SELECT IF(items.item_name='(not set)', NULL,items.item_name) 
            FROM UNNEST(items) AS items 
            ORDER BY items.item_name), 
                ',') AS products_name_75, 


        ARRAY_TO_STRING(ARRAY(
            SELECT IF(items.item_list_name='(not set)', NULL,items.item_list_name)  
            FROM UNNEST(items) AS items 
            ORDER BY items.item_name), 
                ',') AS products_list_name_76, 

        ARRAY_TO_STRING(ARRAY(
            SELECT IF(items.item_category='(not set)', NULL,items.item_category)  
            FROM UNNEST(items) AS items 
            ORDER BY items.item_name), ',') AS products_category_77, 

    FROM 
        `bigquery-for-trakken.analytics_303339971.events_@date` AS ev
),

clean_data AS (
    SELECT *
    FROM get_data
    WHERE fullVisitorId IS NOT NULL AND visitId IS NOT NULL
),

calculate_hit_number AS (
    -- Hit number is calculated based on the event_id
    SELECT 
        tb.*,  
        ROW_NUMBER() OVER(PARTITION BY tb.visitId ORDER BY tb.event_timestamp, tb.event_name) AS hitNumber
    FROM (
        -- ensure that same event and same timestamp and same session is not counted twice 
        SELECT DISTINCT clean_data.visitId, clean_data.event_timestamp, clean_data.event_name, clean_data.event_id  
        FROM clean_data 
    ) AS tb
),

join_query AS (
    SELECT
        clean_data.*,
        calculate_hit_number.hitNumber
    FROM 
        clean_data
    LEFT JOIN 
        calculate_hit_number USING(event_id)
),

calculate_event_metrics AS (
    -- hit_time_60
    -- hit_isExit_61
    -- promoIsView
    -- promoIsClick
    -- hit_isInteraction_62
    SELECT 
        * EXCEPT(first_timestamp, event_name, event_timestamp, event_id),
        (event_timestamp - first_timestamp)/1000000 AS hit_time_60,  
        hitNumber = (MAX(hitNumber) OVER (PARTITION BY visitId)) AS hit_isExit_61  
    FROM (
        SELECT 
            tb.*,  
            MIN(event_timestamp) OVER (PARTITION BY visitId) AS first_timestamp,
            IF(event_name='view_promotion', TRUE, FALSE) AS promoIsView,  
            IF(event_name='select_promotion', TRUE, FALSE) AS promoIsClick, 
            IF(event_name NOT IN ('view_item', 'screenview', 'screen_view', 
            'onboarding_pick_teams_screen_view', 'gdpr_screen_view', 'page_view', 
            'view_item_list', 'view_promotion', 'scroll'), TRUE, FALSE) AS hit_isInteraction_62  
        FROM 
            join_query AS tb) 
)

SELECT DISTINCT *
FROM 
    calculate_event_metrics