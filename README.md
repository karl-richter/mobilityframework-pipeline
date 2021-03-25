# Mobility-Framework Pipeline

## Data Sources
### Mobility data
`s3://mobility-data/mobility-data-raw.csv`

### Weather data
`s3://mobility-data/world-weather-march.csv`  
[Source](https://www.worldweatheronline.com/stuttgart-weather-history/baden-wurttemberg/de.aspx)

## Data Modell
### Staging Layer
- `mobility_staging`  
  Table containing the raw and un-processed mobility data using the schema below:
   ```sql
      city VARCHAR(50),
      country VARCHAR(50),
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
   ``` 
- `weather_staging`  
  Table containing the raw weather data using the schema below:
  ```sql
      date VARCHAR(10),
      city VARCHAR(50),
      country VARCHAR(50),
      temperature_min INTEGER,
      temperature_max INTEGER,
      rain DECIMAL,
      humidity INTEGER
  ```

### Silver Layer
- `mobility_trips`  
  Processed mobility data that contains data on a trip level. The schema is as followed:
  ```sql
      category VARCHAR(15),
      city VARCHAR(25),
      code VARCHAR(15),
      country VARCHAR(25),
      dd VARCHAR(10),
      end_energy DECIMAL,
      end_lat DECIMAL,
      end_lng DECIMAL,
      end_time INTEGER,
      energyLevel_diff DECIMAL,
      energyType VARCHAR(10),
      lastActivity TEXT,
      manufacturer VARCHAR(10),
      mm VARCHAR(7),
      model VARCHAR(10),
      provider VARCHAR(10),
      sign VARCHAR(15),
      start_energy DECIMAL,
      start_lat DECIMAL,
      start_lng DECIMAL,
      start_time INTEGER,
      time_diff DECIMAL,
      time_parsed TEXT,
      type VARCHAR(25),
      yyyy VARCHAR(4)
  ```

- `weather`  
  Processed weather data containing information about the `temperature_avg` and the `weather_type` on a given day. The schema is as followed:
  ```sql
      dd VARCHAR(10),
      city VARCHAR(50),
      country VARCHAR(50),
      temperature_min INTEGER,
      temperature_max INTEGER,
      temperature_avg DECIMAL,
      rain DECIMAL,
      humidity INTEGER,
      weather_type VARCHAR(25)
  ```

### Aggregation Layer
- `base_aggregate`  
   Aggregated mobility data containing information about the number of visible vehicles on a given day, various information about the energy level and the position of the city. For more detailed information, refer to the schema below:
   ```sql
      city VARCHAR(25),
      country VARCHAR(25),
      vehicles_num INTEGER,
      lat DECIMAL,
      lng DECIMAL,
      energy_level_avg DECIMAL,
      energy_level_min DECIMAL,
      energy_level_max DECIMAL,
      dd VARCHAR(10),
      mm VARCHAR(7),
      yyyy VARCHAR(4)
   ```

- `trips_aggregate`  
   Aggregated trip data containing information such as the number of trips on a given day, the number of utilized vehicles and information about the duration. See the schema below for more information:
   ```sql
      city VARCHAR(25),
      country VARCHAR(25),
      type VARCHAR(25),
      trips_num INTEGER,
      utilized_vehicles_num INTEGER,
      trips_duration_avg DECIMAL,
      trips_duration_min DECIMAL,
      trips_duration_max DECIMAL,
      start_energy_avg DECIMAL,
      end_energy_avg DECIMAL,
      temperature_avg DECIMAL,
      weather_type VARCHAR(25),
      dd VARCHAR(10),
      mm VARCHAR(7),
      yyyy VARCHAR(4)
   ```

## Data Pipeline
This section outlines the scope of the individual tasks of this pipeline. Each bullet-point below describes one task of the pipeline. The dependency between the tasks can be derived from the schema below.  

### Staging Layer
   On the staging layer, data is onboarded from S3 to the Redshift data warehouse. The schema of the data is inferred.
   - `transfer_mobility_to_redshift`  
      Load all data of the partition for the specified execution date of the pipeline from the bucket `s3://mobility-data/world-weather-march.csv` into the table `mobility_staging` on Redshift using the provided schema.
   - `transfer_weather_to_redshift`  
      Load all data from within the bucket `s3://mobility-data/world-weather-march.csv` into the table `weather_staging` on Redshift using the provided schema.

### Silver Layer
   On the silver layer, data from the staging layer is processed and stored back into output tables. This layer frames the core of the pipeline as business logic is applied.  
   - `calculate_trips`  
     Extract: Load data from table `mobility_staging`.  
     Transform: Calculate trips according to the logic within the custom Operator to derive `rides`, `maintenance` and `charge` trips from the timeseries data.  
     Load: Store results in the table `mobility_trips` using the provided schema.  
   - `transform_weather`  
     Extract: Load data from `weather_staging`.  
     Transform: Derive average temperature and weather column using SQL.  
     Load: Store data in table `weather` using the provided schema. 

### Aggregation Layer
   On the aggregation layer, data from the tables created on the silver layer is aggregated into tables that provide metrics for potential BI systems that can be connected.  
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