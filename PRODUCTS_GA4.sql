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
    event_timestamp, 
    event_name,
    items,
    CONCAT(user_pseudo_id, get_event('ga_session_id', ev.event_params).int_value, event_name, event_timestamp) AS event_id
FROM 
    `bigquery-for-trakken.analytics_303339971.events_@date` AS ev
),

clean_data AS (
    SELECT *
    FROM get_data
    WHERE fullVisitorId IS NOT NULL AND visitId IS NOT NULL
),

calculate_hit_number AS (
    -- event number is calculated based on the event_id
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
        items.item_name AS products_name,
        items.item_list_name AS products_list_name,
        items.item_category AS products_category,
        event_name,
        event_id
    FROM 
        clean_data,
        unnest(items) as items
),

clean_item_names AS (
    SELECT *
    FROM (
        SELECT
            unnest_items.* EXCEPT(products_name,products_list_name ,products_category),
            IF(products_name='(not set)', NULL, products_name ) AS products_name,
            IF(products_list_name='(not set)', NULL, products_list_name ) AS products_list_name,
            IF(products_category='(not set)', NULL, products_category ) AS products_category
        FROM 
            unnest_items)
    WHERE 
        products_name IS NOT NULL OR products_list_name IS NOT NULL OR products_category IS NOT NULL
),

join_query AS (
    SELECT clean_item_names.*, 
        calculate_hit_number.hitNumber
    FROM clean_item_names
    LEFT JOIN calculate_hit_number USING(event_id)
)

SELECT DISTINCT * EXCEPT(event_id, event_name)
FROM join_query




