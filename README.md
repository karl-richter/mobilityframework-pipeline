# Mobility-Framework Pipeline

## Data Sources
### Mobility data
`s3://mobility-data/mobility-data-raw.csv`

### Weather data
`s3://mobility-data/world-weather-march.csv`  
[Source](https://www.worldweatheronline.com/stuttgart-weather-history/baden-wurttemberg/de.aspx)

## Data Modell
### Staging Layer
- Mobilty Staging Table  
  > mobility_staging

- Weather Staging Table  
  > weather_staging

### Silver Layer
- Trips Table  
  > mobility_trips

- Weather Table  
  > weather

### Aggregation Layer
- Aggregation Base Table  
  > base_aggregate

- Aggregation Trips Table  
  > trips_aggregate

## Data Pipeline
This section outlines the scope of the individual tasks of this pipeline. Each bullet-point below describes one task of the pipeline. The dependency between the tasks can be derived from the schema below.
1. Staging Layer  
   On the staging layer, date is onboarded from S3 to the Redshift data warehouse. The schema of the data is inferred.
   - `transfer_mobility_to_redshift`  
      Load all data of the partition for the specified execution date of the pipeline from the bucket `s3://mobility-data/world-weather-march.csv` into the table `mobility_staging` on Redshift using the provided schema.
   - `transfer_weather_to_redshift`  
      Load all data from within the bucket `s3://mobility-data/world-weather-march.csv` into the table `weather_staging` on Redshift using the provided schema.

2. Silver Layer  
   - `calculate_trips`  
     Extract: Load data from table `mobility_staging`  
     Transform: Calculate trips according to the logic within the custom Operator to derive `rides`, `maintenance` and `charge` trips from the timeseries data.  
     Load: Store results in the table `mobility_trips` using the provided schema.  
   - `transform_weather`  
     Extract: Load data from `weather_staging`  
     Transform: Derive average temperature and weather column using SQL.
     Load: Store data in table `weather` using the provided schema. 

3. Aggregation Layer  
   Aggregate information into Metrics Table.  
   - `aggregate_base`  
     Aggregate the raw mobility data in the table `mobility_staging` to derive the number of vehicles visible on the execution date of the pipeline.
   - `aggregate_trips`  
     Aggregate the trips data in the table `mobility_trips` to derive the number of trips and the average-min-max duration on the execution date of the pipeline.

![DAG Schema](https://github.com/karl-richter/mobilityframework-pipeline/blob/main/img/dag-schema.png)

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