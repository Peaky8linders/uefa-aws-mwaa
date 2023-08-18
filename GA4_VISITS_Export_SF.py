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
COPY INTO UEFA_DEV_DWH.ODS.GOOGLEANALYTICS_VISITS_GA_SESSIONS
FROM @GA_INGESTION/visits_ga4 
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
    uri='gs://ict-ga-bucket/visits_ga4/@file*.csv.gz',
    format='CSV',
    compression='GZIP',
    overwrite=TRUE
) AS
"""    
    
GA_QUERY_VISITS = f"""
WITH 
get_data AS (
SELECT 
    user_pseudo_id AS fullVisitorId,
    CONCAT(user_pseudo_id, `bigquery-for-trakken.analytics_303339971.get_event`('ga_session_id', ev.event_params).int_value) AS visitId,  
    PARSE_DATE("%Y%m%d", event_date) AS date,
    event_timestamp,
    event_name,
    CONCAT(user_pseudo_id, `bigquery-for-trakken.analytics_303339971.get_event`('ga_session_id', ev.event_params).int_value, event_name, event_timestamp) AS event_id, 
    CASE 
        WHEN LOWER(platform)='web' THEN 'Website' 
        WHEN LOWER(platform) in ('android', 'ios') THEN 'App'
        ELSE NULL END AS platform, --platform_id_10,

    `bigquery-for-trakken.analytics_303339971.data_cleaning`(device.category) AS device_category, --device_category_24,
    `bigquery-for-trakken.analytics_303339971.data_cleaning`(geo.country) AS country, --country_name_30,
    `bigquery-for-trakken.analytics_303339971.data_cleaning`(geo.city) AS city, --City_Name_65,
    NULL AS City_ID_64,
    `bigquery-for-trakken.analytics_303339971.data_cleaning`(`bigquery-for-trakken.analytics_303339971.get_event`('country', ev.event_params).string_value) AS country_code, --country_code_uefa_30, 
    `bigquery-for-trakken.analytics_303339971.get_user`('login_status', ev.user_properties).string_value AS login_status, 

    ev.collected_traffic_source.manual_campaign_id AS channel_id, --acq_channel_id_12,

    IF(LOWER(ev.collected_traffic_source.manual_source)='google' AND 
            LOWER(ev.collected_traffic_source.manual_medium)='organic' AND 
            ev.collected_traffic_source.gclid IS NOT NULL, 
        'cpc',
        LOWER(ev.collected_traffic_source.manual_medium)) AS medium, --Custom_Acq_Channel_Name,
            
    ev.collected_traffic_source.manual_campaign_name AS campaign, --Campaign_56,
    ev.collected_traffic_source.manual_source AS source, --Traffic_Source_62,
    `bigquery-for-trakken.analytics_303339971.data_cleaning`(device.language)	AS language, --Device_Language_87,
    NULL AS Gigya_Id_88,
    `bigquery-for-trakken.analytics_303339971.data_cleaning`(device.operating_system)	AS OS, --operatingSystemVersion

    `bigquery-for-trakken.analytics_303339971.get_event`('engagement_time_msec', ev.event_params).int_value AS engagement_time
FROM 
    `bigquery-for-trakken.analytics_303339971.events_@date` AS ev
),

clean_data AS (
    SELECT *,
        MAX(event_timestamp) OVER (PARTITION BY visitId) AS max_event_timestamp,
        MIN(event_timestamp) OVER (PARTITION BY visitId) AS min_event_timestamp
    FROM get_data
    WHERE fullVisitorId IS NOT NULL AND visitId IS NOT NULL
),

session_dimensions AS (
    SELECT DISTINCT
        * EXCEPT(platform, login_status, channel_id, event_timestamp, event_name, event_id, campaign, source, medium, 
        country, country_code, city, device_category, language, OS, max_event_timestamp, min_event_timestamp, date),

        MIN(date) OVER (PARTITION BY visitId) AS date_5, 

        FIRST_VALUE(platform) OVER 
            (PARTITION BY  visitId 
                ORDER BY IF(CONCAT(platform, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS platform_id_10,

        FIRST_VALUE(channel_id) OVER 
            (PARTITION BY  visitId 
                ORDER BY IF(CONCAT(source, medium, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS acq_channel_id_12,
        FIRST_VALUE(campaign) OVER 
            (PARTITION BY  visitId 
                ORDER BY IF(CONCAT(source, medium, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS Campaign_56,
        FIRST_VALUE(source) OVER 
            (PARTITION BY  visitId 
                ORDER BY IF(CONCAT(source, medium, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS Traffic_Source_62,
        FIRST_VALUE(medium) OVER 
            (PARTITION BY  visitId 
                ORDER BY IF(CONCAT(source, medium, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS Custom_Acq_Channel_Name,
        FIRST_VALUE(country) OVER
            (PARTITION BY visitId
                ORDER BY IF(CONCAT(country,country_code, city, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS country_name_30,
        FIRST_VALUE(country_code) OVER
            (PARTITION BY visitId
                ORDER BY IF(CONCAT(country,country_code, city, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS country_code_uefa_30,
        FIRST_VALUE(city) OVER
            (PARTITION BY visitId
                ORDER BY IF(CONCAT(country,country_code, city, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS City_Name_65,
        FIRST_VALUE(device_category) OVER
            (PARTITION BY visitId
                ORDER BY IF(CONCAT(device_category, language,OS, event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS device_category_24,
        FIRST_VALUE(language) OVER
            (PARTITION BY visitId
                ORDER BY IF(CONCAT(device_category, language,OS,event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS Device_Language_87,
        FIRST_VALUE(OS) OVER
            (PARTITION BY visitId
                ORDER BY IF(CONCAT(device_category, language,OS,event_timestamp) IS NULL, max_event_timestamp+1, event_timestamp)) AS operatingSystemVersion,

        (max_event_timestamp - min_event_timestamp)/1000000 AS session_duration,

        MAX(IF(event_name IN ('first_visit','first_open'),1,0)) OVER (PARTITION BY visitId)  AS newVisits,

        COUNT(DISTINCT IF(event_name='page_view', event_timestamp, NULL)) OVER (PARTITION BY visitId) AS pageviews_44,

        COUNT(DISTINCT IF(event_name='screen_view', event_timestamp, NULL)) OVER (PARTITION BY visitId) AS screenviews_44,

        MAX(IF(event_name NOT IN ('view_item', 'screenview', 'screen_view', 
            'onboarding_pick_teams_screen_view', 'gdpr_screen_view', 'page_view', 
            'view_item_list', 'view_promotion', 'scroll'), 1, 0)) OVER (PARTITION BY visitId) AS total_visit_51,

        CAST(MAX(IF(LOWER(login_status) IN ('logged in', 'true', '1'), 1, 0)) OVER (PARTITION BY visitId) AS BOOLEAN) AS is_login, 
        
        COUNT(DISTINCT event_id) OVER (PARTITION BY visitId) AS hits
    FROM 
        clean_data
),

create_time_on_screen_page AS (
    SELECT 
        * EXCEPT(session_duration),
        IF(is_login=TRUE, 'true', 'false' ) AS nbr_loggedin_user_46,
        IF(platform_id_10='Website', session_duration, NULL) AS timeonesite,
        IF(platform_id_10='App', session_duration, NULL) AS timeOnScreen,
        IF(Custom_Acq_Channel_Name IS NULL AND Campaign_56 IS NULL AND  Traffic_Source_62 IS NULL,
            NULL, 
            `bigquery-for-trakken.analytics_303339971.channel_grouping`(`bigquery-for-trakken.analytics_303339971.create_channel`(Custom_Acq_Channel_Name, Campaign_56,Traffic_Source_62 ))) AS channel_name_13
    FROM 
        session_dimensions 
)

SELECT DISTINCT * 
FROM create_time_on_screen_page;
"""

SNOWFLAKE_CONN_ID = "uefa_snowflake_conn"
BIGQUERY_CONN_ID = "uefa_bigquery_conn"
BQ_PROJECT_ID = "bigquery-for-trakken"
LOCATION = "EU"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "BIGQUERY_GA4_VISITS_EXPORT_SNOWFLAKE"

def ga_dq_checks():
    table='VISITS'
    query=GA_QUERY_VISITS
        
with DAG(
    DAG_ID,
    start_date=datetime(2023, 8, 8),
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
                "query": GA_EXPORT_CMD.replace('@file','VISITS') + GA_QUERY_VISITS.replace('@date',datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')),
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