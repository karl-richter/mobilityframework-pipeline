from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List

class CalculateTripsOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 source_table="",
                 destination_table="",
                 truncate_insert=False,
                 config={},
                 create_table="",
                 *args, **kwargs):

        super(CalculateTripsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.source_table = source_table
        self.destination_table = destination_table
        self.truncate_insert = truncate_insert
        self.config = config
        self.create_table = create_table
    
    def parse_time (data, column_name = 'time'):
        """
        Takes a DataFrame and parses a provided timestamp column to unixtime.

        :param data
        :param column_name
        :return data
        """

        data[column_name + '_parsed'] = pd.to_datetime(data[column_name], unit = 's')

        return data

    def normalize_column (data, column_name: str, min_value = 0, max_value = 100):
        """
        Takes a DataFrame and creates a new normed column around a provided min max value range.

        :param data
        :param column_name
        :param min_value
        :param max_value
        :return data
        """

        data[column_name] = round((data[column_name] - min_value) / (max_value - min_value) * 100, 1)

        return data
    
    def derive_delta (data, column_names: List[str]):
        """
        
        :param data
        :param column_name
        :return data
        """
        for column_name in column_names:
            data[column_name + '_prev'] = data.sort_values('time').groupby('sign')[column_name].shift()
            data[column_name + '_diff'] = data[column_name] - data[column_name + '_prev']

            data = data.drop(columns=[column_name + '_prev'])

        return data

    def identify_trips (data, maintenance_threshold: int, charge_threshold: int):
        """
        ...
        """

        conditions = [ 
            (data['time_diff'] >= maintenance_threshold),
            (data['time_diff'] >= 150) & \
             ((data['lat_diff'].abs() >= 0.0005) | (data['lng_diff'].abs() >= 0.0005)) & \
             (data['energyLevel_diff'] <= 0),
            (data['time_diff'] >= 150) & (data['energyLevel_diff'] >= charge_threshold)]
        
        choices = [ "maintenance", "ride", "charge"]
            
        data["type"] = np.select(conditions, choices, default = "_")
        
        return data

    def calculate_trips (data):
        """
        ...
        """

        data = data[data['type'] != '_']

        data['start_time'] = data['time'] - data['time_diff']
        data['start_lat'] = data['lat'] - data['lat_diff']
        data['start_lng'] = data['lng'] - data['lng_diff']
        data['start_energy'] = data['energyLevel'] - data['energyLevel_diff']
        data = data.rename(columns = {
                            "time": "end_time", 
                            "lat": "end_lat", 
                            "lng": "end_lng", 
                            "energyLevel": "end_energy"
                        }, errors="raise")

        data = data.drop(columns=['lat_diff', 'lng_diff'])

        data = data.reindex(sorted(data.columns), axis=1)

        return data

    def execute(self, context):
        execution_date = context.get('execution_date').strftime("%Y-%m-%d")

        self.log.info(f"Getting Postgres Hook for { self.redshift_conn_id }.")
        self.log.info('GET Redshift Hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating { self.destination_table } table if not exists using the provided schema.")
        redshift.run(self.create_table)

        self.log.info(f"Delete data from potential prior runs from { self.destination_table } for execution date.")
        redshift.run(f"DELETE FROM { self.destination_table } WHERE dd = '{ execution_date }'")

        self.log.info(f"Load from { self.source_table } for execution date.")
        df = redshift.get_pandas_df(sql=f"SELECT * FROM { self.source_table } WHERE dd = '{ execution_date }'")

        self.log.info(f"Transform data using the provided Python function.")

        self.log.info(f"Wrangle data.")
        df = df.rename(columns={'energylevel': 'energyLevel'})
        df = CalculateTripsOperator.parse_time (df)
        if (self.config['normalize_energy']):
            df = CalculateTripsOperator.normalize_column (df, 'energyLevel', 0, df['energyLevel'].max())
        
        self.log.info(f"Derive deltas and identify trips.")
        df = CalculateTripsOperator.derive_delta (df, ['energyLevel', 'lat', 'lng', 'time'])
        df = CalculateTripsOperator.identify_trips (df, maintenance_threshold = self.config['maintenance_threshold'],\
                                                    charge_threshold = self.config['charge_threshold'])

        self.log.info(f"Calculate Trips")
        ride_df = CalculateTripsOperator.calculate_trips (df)
        ride_df = ride_df[ride_df["dd"] == execution_date]
        ride_df['time_parsed'] = ride_df['time_parsed'].astype(str)
        
        self.log.info(f"Insert transformed results into { self.destination_table }.")
        rows = list(ride_df.itertuples(index=False, name=None))
        redshift.insert_rows( table = self.destination_table, rows = rows, target_fields = list(ride_df.columns))

        self.log.info(f"Finished { self.destination_table } table.")
