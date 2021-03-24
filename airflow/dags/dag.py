from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Karl Richter',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30)
}

dag = DAG('sparkify-pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

'''
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_events",
    dag = dag, 
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/{year}/{month:02d}/{year}-{month:02d}-{day:02d}-events.json",
    json_format = "s3://udacity-dend/log_json_path.json",
    redshift_conn_id = "redshift",
    table = "staging_events",
    truncate_insert = True,
    provide_context=True,
    create_table = """
                    CREATE TABLE IF NOT EXISTS staging_events (
                        artist varchar(256),
                        auth varchar(256),
                        firstname varchar(256),
                        gender varchar(256),
                        iteminsession int4,
                        lastname varchar(256),
                        length numeric(18,0),
                        "level" varchar(256),
                        location varchar(256),
                        "method" varchar(256),
                        page varchar(256),
                        registration numeric(18,0),
                        sessionid int4,
                        song varchar(256),
                        status int4,
                        ts int8,
                        useragent varchar(256),
                        userid int4
                    );"""
)
'''

'''
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table ="songplays",
    truncate_insert = True,
    query = SqlQueries.songplay_table_insert,
    create_table = """
                   CREATE TABLE IF NOT EXISTS songplays (
                      playid varchar(32) NOT NULL,
                      start_time timestamp NOT NULL,
                      userid int4 NOT NULL,
                      "level" varchar(256),
                      songid varchar(256),
                      artistid varchar(256),
                      sessionid int4,
                      location varchar(256),
                      user_agent varchar(256),
                      CONSTRAINT songplays_pkey PRIMARY KEY (playid)
                   );
                   """
)
'''

# DEPENDENCIES
