# mobilityframework-pipeline

## Scope the Project and Gather Data
## Explore and Assess the Data
## Define the Data Model
## Run ETL to Model the Data
## Complete Project Write Up

## Dependencies
pip install google-cloud-bigquery

## Run Apache Airflow
1. (Optional) Install Airflow Package
   `pip install apache-airflow==1.10.15`
2. (Optional) Export Airflow default environment
   `export AIRFLOW_HOME=~/airflow`
3. (Optional) Initialize Airflow Metadatabase
   `airflow initdb`
4. Start Airflow Web Server in seperate Terminal
   `airflow webserver -p 8080`
5. Start Airflow Scheduler in seperate Terminal
   `airflow scheduler`
6. Check that Airflow detected DAG
   `airflow tasks list sparkify-pipeline --tree`
7. Run task from CLI
   `airflow tasks test <dag_id> <task_id> <execution_date>`