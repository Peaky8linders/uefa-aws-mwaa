
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

WITH 
get_data AS (
    SELECT 
        CONCAT(user_pseudo_id, get_event('ga_session_id', ev.event_params).int_value) AS visitId,
        user_pseudo_id AS fullVisitorId,
        event_name,
        event_timestamp,
        CONCAT(user_pseudo_id, get_event('ga_session_id', ev.event_params).int_value, event_name, event_timestamp) AS event_id,
        items
    FROM 
        `bigquery-for-trakken.analytics_303339971.events_@date` AS ev
),

clean_data AS (
    SELECT *
    FROM get_data
    WHERE fullVisitorId IS NOT NULL AND visitId IS NOT NULL
),

calculate_hit_number AS (
    -- hit number is calculated based on the event_id
    SELECT 
        tb.*,  
        ROW_NUMBER() OVER(PARTITION BY tb.visitId ORDER BY tb.event_timestamp, tb.event_name) AS hitNumber
    FROM (
        -- ensure that same event and same timestamp and same session is not counted twice
        SELECT DISTINCT clean_data.* EXCEPT(items)
        FROM clean_data 
    ) AS tb
),

unnest_items AS (
    SELECT 
        visitId,
        fullVisitorId,
        items.promotion_id AS promoId,
        items.promotion_name AS promoName,
        event_name,
        IF(event_name='view_promotion', TRUE, FALSE) AS promoIsView,
        IF(event_name='select_promotion', TRUE, FALSE) AS promoIsClick,
        event_id
    FROM 
        clean_data,
        unnest(items) AS items
),

clean_promo_names AS (
    SELECT *
    FROM (
        SELECT
            unnest_items.* EXCEPT(promoId,promoName),
            IF(promoId='(not set)', NULL, promoId ) AS promoId,
            IF(promoName='(not set)', NULL, promoName ) AS promoName
        FROM 
            unnest_items)
    WHERE 
        promoId IS NOT NULL AND promoName IS NOT NULL 
),

join_query AS (
    SELECT 
        clean_promo_names.*, 
        calculate_hit_number.hitNumber
    FROM clean_promo_names
    LEFT JOIN calculate_hit_number USING(event_id)
)

SELECT DISTINCT * EXCEPT(event_id, event_name)
FROM join_query 
WHERE event_name IN ('view_promotion', 'select_promotion')
