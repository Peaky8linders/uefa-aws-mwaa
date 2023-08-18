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
COPY INTO UEFA_DEV_DWH.ODS.GOOGLEANALYTICS_PROMOTIONS_GA_SESSIONS
FROM @GA_INGESTION/promo_ga4 
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
    uri='gs://ict-ga-bucket/promo_ga4/@file*.csv.gz',
    format='CSV',
    compression='GZIP',
    overwrite=TRUE
) AS
"""    
    
GA_QUERY_PROMO = f"""
WITH 
get_data AS (
    SELECT 
        CONCAT(user_pseudo_id, `bigquery-for-trakken.analytics_303339971.get_event`('ga_session_id', ev.event_params).int_value) AS visitId,
        user_pseudo_id AS fullVisitorId,
        PARSE_DATE("%Y%m%d", event_date) AS date,
        event_name,
        event_timestamp,
        CONCAT(user_pseudo_id, `bigquery-for-trakken.analytics_303339971.get_event`('ga_session_id', ev.event_params).int_value, event_name, event_timestamp) AS event_id,
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
        MIN(date) OVER (PARTITION BY visitId) AS date_5,
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
WHERE event_name IN ('view_promotion', 'select_promotion');
"""

SNOWFLAKE_CONN_ID = "uefa_snowflake_conn"
BIGQUERY_CONN_ID = "uefa_bigquery_conn"
BQ_PROJECT_ID = "bigquery-for-trakken"
LOCATION = "EU"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "BIGQUERY_GA4_PROMO_EXPORT_SNOWFLAKE"

def ga_dq_checks():
    table='PROMO'
    query=GA_QUERY_PROMO
        
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
                "query": GA_EXPORT_CMD.replace('@file','PROMO') + GA_QUERY_PROMO.replace('@date',datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')),
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