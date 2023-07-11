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
COPY INTO UEFA_DEV_IDP.GA_PILOT.GA_HITS_GA_SESSIONS_MWAA
from @GA_INGESTION/hits
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
    uri='gs://ict-ga-bucket/hits/@file*.csv.gz',
    format='CSV',
    compression='GZIP',
    overwrite=TRUE
) AS
"""    
    
GA_QUERY_HITS = f"""
SELECT DISTINCT
data.visitId,
data.fullVisitorId,
hits.hitNumber,
hits.sourcePropertyInfo.sourcePropertyTrackingId as product_id_14,
hits.sourcePropertyInfo.sourcePropertyDisplayName as product_name_displayname_15,
hits.page.hostname as product_name_hostname_15,
competition.value as competition_16,
page_name.value as page_name_17,
hits.page.pageTitle as page_name_18,
hits.appInfo.screenName as screen_name_19,
hits.sourcePropertyInfo.sourcePropertyTrackingId as Source_Property_Tracking_ID_20,
hits.page.hostname as hostname_21 ,
hits.sourcePropertyInfo.sourcePropertyDisplayName as Source_Property_Display_Name_22,
language.value as language_26,
article_master_id.value as article_master_id_27,
article_child_id.value as article_child_id_27,
page_type.value as page_type_28,
hits.eventInfo.eventCategory as event_category,
hits.eventInfo.eventAction as event_action,
tvplatform.value as TV_Platforms_55,
match.value as Match_ID,
matchname.value as Match_Name,
hits.eventInfo.eventLabel as event_label_57,
videotype.value as videoType_58,
hits.type as hit_type_59,
hits.time as hit_time_60,
hits.isExit as hit_isExit_61,
hits.isInteraction as hit_isInteraction_62,
team.value as Team_ID_63,
videoid.value as video_id_68,
article_master_name.value as article_master_name_69,
article_child_name.value as articleChildName_69,
videoname.value as video_name_70,
user_news_letter.value as user_news_letter_71,
user_team.value as user_team_72,
video_length.value as video_length_73,
player_id.value as player_id_74,
exclusive_promo_id.value as exclusive_promo_id_75,
ARRAY_TO_STRING(ARRAY(SELECT promo.promoId FROM UNNEST(hits.promotion) as promo ORDER BY promo.promoId), ',') as promoId,
ARRAY_TO_STRING(ARRAY(SELECT promo.promoName FROM UNNEST(hits.promotion) as promo ORDER BY promo.promoId), ',') as promoName,
hits.promotionActionInfo.promoIsView,
hits.promotionActionInfo.promoIsClick,
ARRAY_TO_STRING(ARRAY(SELECT product.v2ProductName FROM UNNEST(hits.product) as product ORDER BY product.v2ProductName) , ',') as products_name_75,
ARRAY_TO_STRING(ARRAY(SELECT product.productListName FROM UNNEST(hits.product) as product ORDER BY product.v2ProductName), ',') as products_list_name_76,
ARRAY_TO_STRING(ARRAY(SELECT product.v2ProductCategory FROM UNNEST(hits.product) as product ORDER BY product.v2ProductName), ',') as products_category_77
FROM `176742125.ga_sessions_@date` data 
CROSS JOIN UNNEST(data.hits) as hits 
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS language on language.index = 1
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS page_type on page_type.index = 4
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS competition on competition.index = 10
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS page_name on page_name.index = 14
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS user_news_letter on user_news_letter.index = 25
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS user_team on user_team.index = 30
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS user_club on user_club.index = 31
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS videoname on videoname.index = 35
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS videoid on videoid.index = 36
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS video_length on video_length.index = 37
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS article_master_id on article_master_id.index = 39
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS article_master_name on article_master_name.index = 40
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS article_child_id on article_child_id.index = 41
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS article_child_name on article_child_name.index = 42
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS match on match.index = 43
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS matchname on matchname.index = 44
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS player_id on player_id.index = 51
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS team on team.index = 53
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS exclusive_promo_id on exclusive_promo_id.index = 56
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS videotype on videotype.index = 105
LEFT OUTER JOIN UNNEST(hits.customDimensions) AS tvplatform on tvplatform.index = 106
"""

SNOWFLAKE_CONN_ID = "uefa_snowflake_conn"
BIGQUERY_CONN_ID = "uefa_bigquery_conn"
BQ_PROJECT_ID = "bigquery-for-trakken"
LOCATION = "US"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "BIGQUERY_GA_HITS_EXPORT_SNOWFLAKE"

def ga_dq_checks():
    table='HITS'
    query=GA_QUERY_HITS
        
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