from __future__ import annotations

import os
from datetime import datetime, timedelta
import logging
import sys
import boto3
import time

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError

import snowflake.connector

# Import google bigquery and google client dependencies
from google.cloud import bigquery
from google.oauth2 import service_account

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryColumnCheckOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryTableCheckOperator,
    BigQueryValueCheckOperator,
)

#from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#BigQueryOperator.operator_extra_links = None

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

SF_COPY_QUERY = f"""
USE WAREHOUSE UEFA_DEV_BI_TEAM_VWH;
USE UEFA_DEV_DWH;
USE SCHEMA ODS;
COPY INTO UEFA_DEV_DWH.ODS.GOOGLEANALYTICS_HITS_GA_SESSIONS FROM
( SELECT 
$2 ,
$1 ,
$48 ,
$42 ,
$3 ,
$4 ,
$5 ,
$6 ,
$7,
$8,
$9,
$10 ,
$11,
$12,
$13,
$14,
$15,
$16,
$17,
$18,
$19,
$20,
$21,
$22,
$23,
$24,
$46,
$44,
$43,
$25, 
$26,
$27,
$28,
$29,
$30,
$31,
$32,
$33, 
$34,
$35, 
$36,
$45,
$47,
$37,
$38,
$39,
$40,
$41
FROM @GA_INGESTION/hits_ga4)
ON_ERROR='CONTINUE' 
FILE_FORMAT=(type=csv field_delimiter=',' compression=gzip 
			field_optionally_enclosed_by='"' ESCAPE_UNENCLOSED_FIELD = None 
			ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE TRIM_SPACE=TRUE 
			EMPTY_FIELD_AS_NULL = TRUE NULL_IF = ('')
			) 
force=TRUE;
"""

GA_EXPORT_CMD = """
EXPORT DATA OPTIONS (
    uri='gs://ict-ga-bucket/hits_ga4/@file*.csv.gz',
    format='CSV',
    compression='GZIP',
    overwrite=TRUE
) AS
"""    
    
GA_QUERY_HITS = f"""
WITH
get_data AS (
    SELECT 
        user_pseudo_id AS fullVisitorId, 
        CONCAT(user_pseudo_id, `bigquery-for-trakken.analytics_303339971.get_event`('ga_session_id', ev.event_params).int_value) AS visitId, 
        event_name,
        event_timestamp,
        CONCAT(user_pseudo_id, `bigquery-for-trakken.analytics_303339971.get_event`('ga_session_id', ev.event_params).int_value, event_name, event_timestamp) AS event_id,

        stream_id AS product_id_14,  
        NULL as product_name_displayname_15,

        `bigquery-for-trakken.analytics_303339971.get_event`('link_domain', ev.event_params).string_value AS product_name_hostname_15, 

        `bigquery-for-trakken.analytics_303339971.get_event`('competition', ev.event_params).string_value AS competition_16, 
        --page_name did not have the names there where registered in UA
        `bigquery-for-trakken.analytics_303339971.get_event`('page_path', ev.event_params).string_value AS page_name_17,

        --although not all names showed
        `bigquery-for-trakken.analytics_303339971.get_event`('page_title', ev.event_params).string_value AS page_name_18, 
        -- screen_name (values did not match check)
        `bigquery-for-trakken.analytics_303339971.get_event`('screen_name', ev.event_params).string_value AS screen_name_19, 
        
        --the dimension as previous
        NULL AS Source_Property_Tracking_ID_20,
        NULL AS hostname_21,
        NULL AS Source_Property_Display_Name_22, 

        UPPER(`bigquery-for-trakken.analytics_303339971.get_event`('page_language', ev.event_params).string_value) AS language_26,
        `bigquery-for-trakken.analytics_303339971.get_event`('article_master_id', ev.event_params).string_value AS article_master_id_27, 
        `bigquery-for-trakken.analytics_303339971.get_event`('article_child_id', ev.event_params).string_value AS article_child_id_27, 
        `bigquery-for-trakken.analytics_303339971.get_event`('page_type', ev.event_params).string_value AS page_type_28,

        `bigquery-for-trakken.analytics_303339971.get_event`('event_category', ev.event_params).string_value AS event_category,
        `bigquery-for-trakken.analytics_303339971.get_event`('event_action', ev.event_params).string_value AS event_action,
        IF(device.category='smart tv', device.operating_system, NULL) AS TV_Platforms_55,
        `bigquery-for-trakken.analytics_303339971.get_event`('match_id', ev.event_params).string_value AS Match_ID, 
        `bigquery-for-trakken.analytics_303339971.get_event`('match_name', ev.event_params).string_value AS Match_Name, 
        `bigquery-for-trakken.analytics_303339971.get_event`('event_label', ev.event_params).string_value AS event_label_57, 

        `bigquery-for-trakken.analytics_303339971.get_event`('video_type', ev.event_params).string_value AS videoType_58, 

        CASE 
            WHEN event_name='page_view' THEN 'PAGE'
            WHEN event_name='screen_view' THEN 'APPVIEW'
        ELSE 'EVENT' END AS hit_type_59, 

        `bigquery-for-trakken.analytics_303339971.get_event`('team_id', ev.event_params).string_value AS Team_ID_63, 

        `bigquery-for-trakken.analytics_303339971.get_event`('video_id', ev.event_params).string_value AS video_id_68, 

        `bigquery-for-trakken.analytics_303339971.get_event`('article_master_title', ev.event_params).string_value AS article_master_name_69,

        `bigquery-for-trakken.analytics_303339971.get_event`('article_child_title', ev.event_params).string_value AS articleChildName_69, 

        `bigquery-for-trakken.analytics_303339971.get_event`('video_title', ev.event_params).string_value AS video_name_70, 

        `bigquery-for-trakken.analytics_303339971.get_user`('newsletter_subscriber', ev.user_properties).string_value AS user_news_letter_71, 

        `bigquery-for-trakken.analytics_303339971.get_user`('user_team', ev.user_properties).string_value AS user_team_72,

        IF(event_name='video_start',`bigquery-for-trakken.analytics_303339971.get_event`('video_duration', ev.event_params).int_value, NULL) AS video_length_73,

        IFNULL(`bigquery-for-trakken.analytics_303339971.get_event`('player_id', ev.event_params).string_value,
            CAST(`bigquery-for-trakken.analytics_303339971.get_event`('player_id', ev.event_params).int_value AS string) ) AS player_id_74, 

        `bigquery-for-trakken.analytics_303339971.get_event`('partners', ev.event_params).string_value AS exclusive_promo_id_75,

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
            
        NULLIF(`bigquery-for-trakken.analytics_303339971.get_event`('content_section_1', ev.event_params).string_value, '') AS content_section_1_85,
        NULLIF(`bigquery-for-trakken.analytics_303339971.get_event`('content_section_2', ev.event_params).string_value, '') AS content_section_2_86

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
        hitNumber = (MAX(hitNumber) OVER (PARTITION BY visitId)) AS hit_isExit_61,
        Extract(date from TIMESTAMP_MICROS(first_timestamp)) as datedate
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
    calculate_event_metrics;
"""

SNOWFLAKE_CONN_ID = "uefa_snowflake_conn"
BIGQUERY_CONN_ID = "uefa_bigquery_conn"
BQ_PROJECT_ID = "bigquery-for-trakken"
LOCATION = "EU"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "BIGQUERY_GA4_HITS_EXPORT_SNOWFLAKE"

def ga_dq_checks():
    table='HITS'
    query=GA_QUERY_HITS
        
with DAG(
    DAG_ID,
    start_date=datetime(2023, 7, 25),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID, "bigquery_conn_id": BIGQUERY_CONN_ID},
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    
    t_export_ga_data_bigquery = BigQueryInsertJobOperator(
        task_id="export_ga_data_bigquery",
        gcp_conn_id=BIGQUERY_CONN_ID,
        project_id=BQ_PROJECT_ID,
        configuration={
            "query": {
                "query": GA_EXPORT_CMD.replace('@file','HITS') + GA_QUERY_HITS.replace('@date',datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')),
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )
    
    snowflake_op_copy = SnowflakeOperator(
        task_id="copy_to_snowflake",
        sql=SF_COPY_QUERY
    )
    
    t_ga_dq_checks = PythonOperator(task_id='ga_dq_checks',
                                    python_callable=ga_dq_checks,
                                    dag=dag,
                                    retries=1)
                               
    t_export_ga_data_bigquery >> snowflake_op_copy
    
    snowflake_op_copy >> t_ga_dq_checks