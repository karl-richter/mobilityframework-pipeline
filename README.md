# Mobility-Framework Pipeline
This project aims at providing a pipeline for loading, transforming and enriching timeseries data by a german micro-mobility provider. The mobility data contains pings of ~750 vehicles for the german city of Stuttgart. Each vehicle got pinged every two minutes throughout the day. Each ping contains information such as the battery level, a geo-location and a unique vehicle identifier (sign). Based on the hypothesis that vehicles do not return a ping while they are rented (not visible in the micro-mobility provider app), trips can be derived by identifying gaps in the timeseries of pings of individual vehicles. The number of trips on a day, the average duration, frequent start and end points can be derived from those trips. The output of this project could be a BI dashboard that displays the previously mentioned metrics to non-technical users.  

The raw dataset for this project is stored on S3, but all tables are desired in a Redshift Data Warehouse. Therefore, steps of this pipeline include the onboarding on the Redshift Data Warehouse, the curation of trips data using the business logic and the aggregation of both trips data and general information that can be derived directly from the dataset without any further processing. This makes up three level of data in the Data Warehouse: the `Staging Layer`, the `Silver Layer` and the `Aggregation Layer`.

To enrich the trip information, a weather dataset is loaded that allows to derive the weather on a given day. This allows to explore correlations between the weather and the number of trips over a period of days.

A plotted extract from the mobility dataset can be seen below:  


![Mobility Data](https://github.com/karl-richter/mobilityframework-pipeline/blob/main/img/mobility-data.png)


## Data Sources
The datasets for this project are located in the S3 bucket `s3://mobility-data` on Amazon Web Services (AWS).

### Mobility data
The main data source for this project are timestamps of vehicles (e-scooters) by a german micro-mobility provider in the city of Stuttgart. The dataset contains an entry for each of the ~750 vehicles every 2 minutes of the day, containing information such as the battery level, the geo-location and a unique identifier. The mobility data is partitioned by year, month and day and stored as Comma Seperated Values (CSVs) `s3://mobility-data/2021/03/___.csv`. For this project, data from the 1st of March to the 10th of March is available. The pipeline will be scheduled to run once a day, reading and processing the CSV partition for the respective execution date at a time.

### Weather data
The data source for enrichment for this project is weather data by the provider `worldweatheronline.com`. The dataset contains one entry for each day containing information such as the min-max temperature, level of rain and humidity in the city of Stuttgart. The data is stored in the bucket `s3://mobility-data/world-weather-march.csv` and was originally extracted from [World Weather Online](https://www.worldweatheronline.com/stuttgart-weather-history/baden-wurttemberg/de.aspx).

## Data Modell
### Staging Layer
- `mobility_staging`  
  Table containing the raw and un-processed mobility data using the schema below:
   ```sql
      city VARCHAR(50),
      country VARCHAR(50),
      lat REAL,
      lng REAL,
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
      rain REAL,
      humidity INTEGER
  ```

### Silver Layer
- `mobility_trips`  
  Processed mobility data that contains data on a trip level. 
  Trips are calculated using a business logic that aims at detecting gaps in the timeseries of individual e-scooters. For example: As each scooter gets pinged every 2 minutes, a response containing a geo-location is expected every 2 minutes. If a scooter has been visible for a few hours at a given location, then disappears, and appears again but potentially at a different location, the "hidden-time" is identified as a `trip`. Depending on certain criterias, such as the change of location, the duration of the disappearence and the change in the scooter charge level, the trip is classified as either a `ride`, a `charge` or a `maintenance` event.  
  The schema of the table is as followed:
  ```sql
      category VARCHAR(15),
      city VARCHAR(25),
      code VARCHAR(15),
      country VARCHAR(25),
      dd VARCHAR(10),
      end_energy REAL,
      end_lat REAL,
      end_lng REAL,
      end_time INTEGER,
      energyLevel_diff REAL,
      energyType VARCHAR(10),
      lastActivity TEXT,
      manufacturer VARCHAR(10),
      mm VARCHAR(7),
      model VARCHAR(10),
      provider VARCHAR(10),
      sign VARCHAR(15),
      start_energy REAL,
      start_lat REAL,
      start_lng REAL,
      start_time INTEGER,
      time_diff REAL,
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
      temperature_avg REAL,
      rain REAL,
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
      lat REAL,
      lng REAL,
      energy_level_avg REAL,
      energy_level_min REAL,
      energy_level_max REAL,
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
      trips_duration_avg REAL,
      trips_duration_min REAL,
      trips_duration_max REAL,
      start_energy_avg REAL,
      end_energy_avg REAL,
      temperature_avg REAL,
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
  `airflow tasks test mobility-pipeline transfer_mobility_to_redshift "2021-03-01"`

## Files in Repository
- airflow
  - dags
    - dag.py (DAG for mobility-pipeline)
    - sql_statements.py (Store of SQL queries to create and insert into tables)
  - plugins
    - operators
      - calculate_trips.py (Operator to derive trips using business logic)
      - s3_to_redshift.py (Operator to load data from S3 to Redshift)
  - airflow.cfg (Airflow Config)
  - unittests.cfg (Airflow Unittests)
- img (Contains images for explanation purposes in this README)
 
- Dockerfile (Dockerfile for the Remote Container as explained in the section `Run Development Environment`)
- requirements.txt (Requirements to satisfy during the building of the docker image)
- .devcontainer (Configurations for the Remote Container)
- README.md