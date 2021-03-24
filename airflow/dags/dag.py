from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3ToRedshiftOperator
#from airflow.operators import (S3ToRedshiftOperator) #, LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Karl Richter',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30)
}

dag = DAG('mobility-pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          max_active_runs=1
        )

start_operator = DummyOperator(
        task_id = 'Begin_execution', 
        dag = dag
    )

task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id = 'transfer_s3_to_redshift',
        dag = dag,
        aws_credentials_id = "aws_credentials",
        redshift_conn_id = "redshift",
        s3_bucket = "mobility-data",
        s3_key = "mobility-data-raw.csv",
        schema = "PUBLIC",
        table = "mobility_staging",
        copy_arguments = "CSV DELIMITER ',' IGNOREHEADER 1",
        create_table = """
                       DROP TABLE IF EXISTS mobility_staging;
                       CREATE TABLE IF NOT EXISTS mobility_staging (
                          city VARCHAR(50),
                          county VARCHAR(50),
                          lat DECIMAL,
                          lng DECIMAL,
                          model VARCHAR(10),
                          sign VARCHAR(10),
                          code VARCHAR(10),
                          energyLevel INTEGER,
                          energyType VARCHAR(10),
                          lastActivity VARCHAR(50),
                          manufacturer VARCHAR(10),
                          provider VARCHAR(10),
                          time INTEGER,
                          yyyy VARCHAR(4),
                          mm VARCHAR(7),
                          dd VARCHAR(10),
                          category VARCHAR(20)
                       )"""
    )

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
start_operator >> task_transfer_s3_to_redshift