from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import S3ToRedshiftOperator
from airflow.operators import CalculateTripsOperator
#from airflow.operators import (S3ToRedshiftOperator) #, LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)

import sql_statements

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Karl Richter',
    'start_date': datetime(2021, 3, 1),
    'end_date': datetime(2021, 3, 2)
}

dag = DAG('mobility-pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          max_active_runs=1
        )

pipeline_start = DummyOperator(
        task_id = 'pipeline_start', 
        dag = dag
    )

task_transfer_mobility_to_redshift = S3ToRedshiftOperator (
        task_id = 'transfer_mobility_to_redshift',
        dag = dag,
        aws_credentials_id = "aws_credentials",
        redshift_conn_id = "redshift",
        s3_bucket = "mobility-data",
        s3_key = "{year}/{month}/{day}/mobility-data-{date}.csv",
        schema = "PUBLIC",
        table = "mobility_staging",
        copy_arguments = "CSV DELIMITER ',' IGNOREHEADER 1",
        create_table = sql_statements.MOBILITY_CREATE_TABLE
)

task_transfer_weather_to_redshift = S3ToRedshiftOperator (
        task_id = 'transfer_weather_to_redshift',
        dag = dag,
        aws_credentials_id = "aws_credentials",
        redshift_conn_id = "redshift",
        s3_bucket = "mobility-data",
        s3_key = "world-weather-march.csv",
        schema = "PUBLIC",
        table = "weather_staging",
        copy_arguments = "CSV DELIMITER ';' IGNOREHEADER 1",
        create_table = sql_statements.WEATHER_CREATE_TABLE
)

task_calculate_trips = CalculateTripsOperator (
    task_id = "calculate_trips",
    dag = dag,
    redshift_conn_id = "redshift",
    source_table = "mobility_staging",
    destination_table = "mobility_trips",
    truncate_insert = False,
    config = {  
                'normalize_energy': False,
                'maintenance_threshold': 18000,
                'charge_threshold': 10 
             },
    create_table = sql_statements.TRIPS_CREATE_TABLE         
)

task_transform_weather = PostgresOperator(
    task_id = "transform_weather",
    postgres_conn_id = "redshift",
    dag = dag,
    sql = """
          {drop}
          {create}
          {insert}
          """.format(drop = sql_statements.WEATHER_TRANS_DROP_TABLE,
                     create = sql_statements.WEATHER_TRANS_CREATE_TABLE,
                     insert = sql_statements.WEATHER_TRANS_INSERT_TABLE
                    )
)

task_aggregate_trips = PostgresOperator(
    task_id = "aggregate_trips",
    postgres_conn_id = "redshift",
    dag = dag,
    sql = """
          {drop}
          {create}
          {delete}
          {insert}
          """.format(drop = sql_statements.AGG_DROP_TABLE,
                     create = sql_statements.AGG_CREATE_TABLE,
                     delete = sql_statements.AGG_DELETE_FROM_TABLE.format(execution_date = '{{ ds }}'),
                     insert = sql_statements.AGG_INSERT_TABLE.format(execution_date = '{{ ds }}')
                    )
)

task_aggregate_base = PostgresOperator(
    task_id = "aggregate_base",
    postgres_conn_id = "redshift",
    dag = dag,
    sql = """
          {drop}
          {create}
          {delete}
          {insert}
          """.format(drop = sql_statements.BASE_DROP_TABLE,
                     create = sql_statements.BASE_CREATE_TABLE,
                     delete = sql_statements.BASE_DELETE_FROM_TABLE.format(execution_date = '{{ ds }}'),
                     insert = sql_statements.BASE_INSERT_TABLE.format(execution_date = '{{ ds }}')
                    )
)

pipeline_end = DummyOperator(
        task_id = 'pipeline_end', 
        dag = dag
)

# DEPENDENCIES
pipeline_start                     >> task_transfer_mobility_to_redshift
pipeline_start                     >> task_transfer_weather_to_redshift
task_transfer_weather_to_redshift  >> task_transform_weather
task_transfer_mobility_to_redshift >> task_aggregate_base
task_transfer_mobility_to_redshift >> task_calculate_trips
task_transform_weather             >> task_aggregate_trips
task_calculate_trips               >> task_aggregate_trips
task_aggregate_trips               >> pipeline_end
task_aggregate_base                >> pipeline_end