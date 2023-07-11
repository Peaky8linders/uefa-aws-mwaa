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
USE UEFA_DEV_IDP;
USE SCHEMA GA_PILOT;
COPY INTO UEFA_DEV_IDP.GA_PILOT.GA_VISITS_GA_SESSIONS_MWAA
FROM @GA_INGESTION/visits 
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
    uri='gs://ict-ga-bucket/visits/@file*.csv.gz',
    format='CSV',
    compression='GZIP',
    overwrite=TRUE
) AS
"""    
    
GA_QUERY_VISITS = f"""
SELECT 
data.fullVisitorId,
data.visitId,  
data.date as date_5,
platform.value as platform_id_10,
data.trafficSource.campaignCode as acq_channel_id_12,
data.channelGrouping as channel_name_13,
data.device.deviceCategory  as device_category_24,
data.geoNetwork.country  as country_name_30,
country_code_uefa.value as country_code_uefa_30,
data.totals.pageviews as pageviews_44,
data.totals.screenviews as screenviews_44,
nbr_loggedin_user.value as  nbr_loggedin_user_46,
data.totals.visits  as total_visit_51,
data.totals.timeOnSite as timeonesite,
data.totals.hits as hits,
data.totals.newVisits as newVisits,
data.totals.timeOnScreen as timeOnScreen,
data.trafficSource.Medium as Custom_Acq_Channel_Name,
data.trafficSource.campaign as Campaign_56,
data.trafficSource.source as Traffic_Source_62,
data.geoNetWork.cityId as City_ID_64,
data.geoNetWork.city as City_Name_65,
FROM `176742125.ga_sessions_@date` data
LEFT OUTER JOIN UNNEST(data.customDimensions) AS country_code_uefa on country_code_uefa.index = 2
LEFT OUTER JOIN UNNEST(data.customDimensions) AS platform on platform.index = 23
LEFT OUTER JOIN UNNEST(data.customDimensions) AS nbr_loggedin_user on  nbr_loggedin_user.index = 24 
"""

SNOWFLAKE_CONN_ID = "uefa_snowflake_conn"
BIGQUERY_CONN_ID = "uefa_bigquery_conn"
BQ_PROJECT_ID = "bigquery-for-trakken"
LOCATION = "US"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "BIGQUERY_GA_VISITS_EXPORT_SNOWFLAKE"

def ga_dq_checks():
    table='VISITS'
    query=GA_QUERY_VISITS
        
with DAG(
    DAG_ID,
    start_date=datetime(2023, 6, 16),
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