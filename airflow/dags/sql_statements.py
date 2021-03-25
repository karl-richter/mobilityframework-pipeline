STAGING_CREATE_TABLE = """
                       DROP TABLE IF EXISTS mobility_staging;
                       CREATE TABLE IF NOT EXISTS mobility_staging (
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
                       )"""

TRIPS_CREATE_TABLE = """
                    CREATE TABLE IF NOT EXISTS mobility_trips (
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
                   );"""

AGG_DROP_TABLE =  """
                 DROP TABLE IF EXISTS trips_aggregate;
                 """

AGG_DELETE_FROM_TABLE =  """
                         DELETE FROM trips_aggregate WHERE dd = '2021-03-01';
                         """

AGG_CREATE_TABLE =  """
                    CREATE TABLE IF NOT EXISTS trips_aggregate (
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
                    dd VARCHAR(10),
                    mm VARCHAR(7),
                    yyyy VARCHAR(4)
                    );
                    """

AGG_INSERT_TABLE =  """
                    INSERT INTO trips_aggregate
                    SELECT 
                        city, 
                        country,
                        type,
                        COUNT(sign) AS trips_num,
                        COUNT(DISTINCT sign) AS utilized_vehicles_num,
                        AVG(time_diff) AS trips_duration_avg,
                        MIN(time_diff) AS trips_duration_min,
                        MAX(time_diff) AS trips_duration_max,
                        AVG(start_energy) AS start_energy_avg,
                        AVG(end_energy) AS end_energy_avg,
                        dd,
                        mm,
                        yyyy
                    FROM  mobility_trips
                    WHERE dd = '2021-03-01'
                    GROUP BY city, country, type, dd, mm, yyyy
                    ;"""

BASE_DROP_TABLE =  """
                 DROP TABLE IF EXISTS base_aggregate;
                 """

BASE_DELETE_FROM_TABLE =  """
                         DELETE FROM base_aggregate WHERE dd = '2021-03-01';
                         """

BASE_CREATE_TABLE =  """
                    CREATE TABLE IF NOT EXISTS base_aggregate (
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
                    );
                    """

BASE_INSERT_TABLE =  """
                    INSERT INTO base_aggregate
                    SELECT 
                        city, 
                        country,
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
                    WHERE dd = '2021-03-01'
                    GROUP BY city, country, dd, mm, yyyy
                    ;"""