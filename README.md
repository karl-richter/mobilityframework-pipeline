# Mobility-Framework Pipeline

## Data Sources
### Mobility data
#### TIER
#### GoRevel

### Weather data

## Data Modell
### Staging Level
- Mobilty Staging Table  
  > staging_table

### Transaction Level
- Trips Table  
  > mobility_trips

### Aggregation Level
- Aggregation Base Table  
  > base_aggregate
- Aggregation Trips Table  
  > trips_aggregate

## ETL Process
1. Load data from S3 to Redshift
2. Calculate Trips and store in Trips Table
3. Aggregate information into Metrics Table
   1. Aggregate Base Table
   2. Aggregate Trips Table

## How to Run the Pipeline
### Run Development Environment
1. (Optional) Install Docker Desktop
2. (Optional) Install Visual Studio Code
3. (Optional) Install VS Code Extension `Remote-Containers`
4. Clone this git Repository
5. Open Repository as Remote Container  
   > VS Code will build a Docker Image using the provided Dockerfile and install all dependencies  
   > Read here for more infos https://code.visualstudio.com/docs/remote/containers
6. Dev Environment is all set up 

### Run Apache Airflow
1. (Optional) Install Airflow Package  
   `pip install apache-airflow==1.10.15`
2. Export Airflow default environment  
   `export AIRFLOW_HOME=/workspaces/mobilityframework-pipeline/airflow`
3. (Optional) Initialize Airflow Metadatabase  
   `airflow initdb`
4. Start Airflow Web Server in seperate Terminal  
   `airflow webserver -p 8080`
5. Start Airflow Scheduler in seperate Terminal  
   `airflow scheduler`
6. Open Airflow Web Server at `localhost:8080` and enter Credentials  
   > Add credentials for AWS ("aws_credentials") and Postgres ("redshift")
#### Optional
- Check that Airflow detected DAG  
  `airflow tasks list mobility-pipeline --tree`
- Run task from CLI  
  `airflow tasks test <dag_id> <task_id> <execution_date>`  
  `airflow tasks test mobility-pipeline transfer_s3_to_redshift "2021-03-01"`