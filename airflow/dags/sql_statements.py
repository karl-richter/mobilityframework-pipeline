MOBILITY_CREATE_TABLE = """
                       CREATE TABLE IF NOT EXISTS mobility_staging (
                          city VARCHAR(50) NOT NULL,
                          country VARCHAR(50) NOT NULL,
                          lat REAL NOT NULL,
                          lng REAL NOT NULL,
                          model VARCHAR(10),
                          sign VARCHAR(10) NOT NULL,
                          code VARCHAR(10),
                          energyLevel INTEGER NOT NULL,
                          energyType VARCHAR(10),
                          lastActivity VARCHAR(50),
                          manufacturer VARCHAR(10),
                          provider VARCHAR(10),
                          time INTEGER NOT NULL,
                          yyyy VARCHAR(4) NOT NULL,
                          mm VARCHAR(7) NOT NULL,
                          dd VARCHAR(10) NOT NULL,
                          category VARCHAR(20)
                       )"""

WEATHER_CREATE_TABLE = """
                       DROP TABLE IF EXISTS weather_staging;
                       CREATE TABLE IF NOT EXISTS weather_staging (
                          date VARCHAR(10) NOT NULL,
                          city VARCHAR(50) NOT NULL,
                          country VARCHAR(50) NOT NULL,
                          temperature_min INTEGER NOT NULL,
                          temperature_max INTEGER NOT NULL,
                          rain REAL NOT NULL,
                          humidity INTEGER NOT NULL,
                          PRIMARY KEY (date)
                       )"""

TRIPS_CREATE_TABLE = """
                    CREATE TABLE IF NOT EXISTS mobility_trips (
                       category VARCHAR(15) NOT NULL,
                       city VARCHAR(25) NOT NULL,
                       code VARCHAR(15),
                       country VARCHAR(25) NOT NULL,
                       dd VARCHAR(10) NOT NULL,
                       end_energy REAL NOT NULL,
                       end_lat REAL NOT NULL,
                       end_lng REAL NOT NULL,
                       end_time INTEGER NOT NULL,
                       energyLevel_diff REAL NOT NULL,
                       energyType VARCHAR(10) NOT NULL,
                       lastActivity TEXT,
                       manufacturer VARCHAR(10),
                       mm VARCHAR(7) NOT NULL,
                       model VARCHAR(10),
                       provider VARCHAR(10),
                       sign VARCHAR(15) NOT NULL,
                       start_energy REAL NOT NULL,
                       start_lat REAL NOT NULL,
                       start_lng REAL NOT NULL,
                       start_time INTEGER NOT NULL,
                       time_diff REAL NOT NULL,
                       time_parsed TEXT,
                       type VARCHAR(25) NOT NULL,
                       yyyy VARCHAR(4) NOT NULL
                   );"""

AGG_DELETE_FROM_TABLE =  """
                         DELETE FROM trips_aggregate WHERE dd = '{execution_date}';
                         """

AGG_CREATE_TABLE =  """
                    CREATE TABLE IF NOT EXISTS trips_aggregate (
                    city VARCHAR(25) NOT NULL,
                    country VARCHAR(25) NOT NULL,
                    type VARCHAR(25) NOT NULL,
                    trips_num INTEGER NOT NULL,
                    utilized_vehicles_num INTEGER NOT NULL,
                    trips_duration_avg REAL,
                    trips_duration_min REAL,
                    trips_duration_max REAL,
                    start_energy_avg REAL,
                    end_energy_avg REAL,
                    temperature_avg REAL,
                    weather_type VARCHAR(25),
                    dd VARCHAR(10) NOT NULL,
                    mm VARCHAR(7) NOT NULL,
                    yyyy VARCHAR(4) NOT NULL
                    );
                    """

AGG_INSERT_TABLE =  """
                    INSERT INTO trips_aggregate
                    SELECT 
                        mobility_trips.city, 
                        mobility_trips.country,
                        type,
                        COUNT(sign) AS trips_num,
                        COUNT(DISTINCT sign) AS utilized_vehicles_num,
                        AVG(time_diff) AS trips_duration_avg,
                        MIN(time_diff) AS trips_duration_min,
                        MAX(time_diff) AS trips_duration_max,
                        AVG(start_energy) AS start_energy_avg,
                        AVG(end_energy) AS end_energy_avg,
                        temperature_avg,
                        weather_type,
                        mobility_trips.dd,
                        mm,
                        yyyy
                    FROM mobility_trips
                    LEFT JOIN weather ON mobility_trips.dd = weather.dd
                    WHERE mobility_trips.dd = '{execution_date}'
                    GROUP BY mobility_trips.city, mobility_trips.country, type, temperature_avg, weather_type, mobility_trips.dd, mm, yyyy
                    ;"""

BASE_DELETE_FROM_TABLE =  """
                         DELETE FROM base_aggregate WHERE dd = '{execution_date}';
                         """

BASE_CREATE_TABLE =  """
                    CREATE TABLE IF NOT EXISTS base_aggregate (
                    city VARCHAR(25) NOT NULL,
                    country VARCHAR(25) NOT NULL,
                    vehicles_num INTEGER NOT NULL,
                    lat REAL,
                    lng REAL,
                    energy_level_avg REAL,
                    energy_level_min REAL,
                    energy_level_max REAL,
                    dd VARCHAR(10) NOT NULL,
                    mm VARCHAR(7) NOT NULL,
                    yyyy VARCHAR(4) NOT NULL
                    );
                    """

BASE_INSERT_TABLE =  """
                    INSERT INTO base_aggregate
                    SELECT 
                        mobility_staging.city, 
                        mobility_staging.country,
                        COUNT(DISTINCT sign) AS vehicles_num,
                        AVG(lat) AS lat,
                        AVG(lng) AS lng,
                        AVG(energyLevel) AS energy_level_avg,
                        MIN(energyLevel) AS energy_level_min,
                        MAX(energyLevel) AS energy_level_max,
                        dd,
                        mm,
                        yyyy
                    FROM  mobility_staging
                    WHERE dd = '{execution_date}'
                    GROUP BY city, country, dd, mm, yyyy
                    ;"""

WEATHER_TRANS_DROP_TABLE =  """
                 DROP TABLE IF EXISTS weather;
                 """

WEATHER_TRANS_CREATE_TABLE =  """
                    CREATE TABLE IF NOT EXISTS weather (
                    dd VARCHAR(10) NOT NULL,
                    city VARCHAR(50) NOT NULL,
                    country VARCHAR(50) NOT NULL,
                    temperature_min INTEGER,
                    temperature_max INTEGER,
                    temperature_avg REAL,
                    rain REAL,
                    humidity INTEGER,
                    weather_type VARCHAR(25),
                    PRIMARY KEY (dd)
                    );
                    """

WEATHER_TRANS_INSERT_TABLE =  """
                    INSERT INTO weather
                    SELECT 
                    date AS dd,
                    city,
                    country,
                    temperature_min,
                    temperature_max,
                    (temperature_max + temperature_min) / 2 AS temperature_avg,
                    rain,
                    humidity,
                    CASE
                        WHEN temperature_avg > 7 THEN 'Very good'
                        WHEN temperature_avg > 5 THEN 'Medium'
                        ELSE 'Bad'
                    END AS weather_type
                    FROM weather_staging
                    ;"""