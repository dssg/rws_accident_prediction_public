import pandas as pd
import numpy as np
import sys
import os
from functools import reduce
from utils.misc_utils import pandas_to_db

class WeatherFeatureFactory(object):
    def __init__(self, knmi_table, conn):
        '''
        
        Level of Aggregation in space depends on the mapping table
        
        Guidelines to create new features:
            - Each Feature should be a new method
            - Name of the function will become name of the feature
            - Use column_name decorator to map which column of hectopunten does
              the feature apply to
            - Each method expects a group of hectopuntens and returns one value for it.
            - If a feature requires multiple columns, @column_name can be custom and for
            our purpose be same as the name of eventual feature/method.
        '''
        
        q = '''select * from {0} as knmi;'''.format(knmi_table)
        
        self.knmi = pd.read_sql(q, con=conn)
        self.knmi.rename(columns={'station_id':'weather_station'}, inplace=True)
        self.knmi.set_index('datetime', inplace=True)
        self.feature_frames = []
        self.full_frame = None
        
    #breaks if all false
    #used in feature_weather_last_event
    def _count_rows_since_true(self, bool_col):
        if bool_col.notnull().sum() == 0:
            e = bool_col.astype(float)
        else:
            first_true_index = bool_col[bool_col == True].index[0]
            a = bool_col.astype(bool)
            b = ~a
            c = b.astype(int)
            d = c.cumsum()
            e = d-d.where(a).ffill().fillna(1).astype(int)
            mask = bool_col.index >= first_true_index
            e = e.where(mask, np.nan)
        return e
          
    def _feature_weather_last_event(self, weather_column):
        #TODO: check the column is binary
        knmi = self.knmi
        
        #define what the new column is called
        col = 'feature_last_{0}_num'.format(weather_column)
        
        combined = []
        
        for station in knmi.weather_station.unique():
            temp = knmi.loc[knmi['weather_station'] == station]
            temp = self._count_rows_since_true(temp[weather_column]).to_frame()
            temp['weather_station'] = station
            temp[col] = temp[weather_column]
            temp[col] = temp[col].shift(1)
            combined.append(temp[['weather_station', col]])
        
        return pd.concat(combined)
    
    def _feature_weather_rain_intensity(self, weather_column, divisor):
        #TODO: make sure it doesn't break on unexpected types
        #TODO: enforce no inf returns
        
        knmi = self.knmi
        
        #define what the new column is called
        col = 'feature_{0}_normby_{1}_num'.format(weather_column, divisor)
        
        #divide the column by divisor
        knmi[col] = knmi[weather_column]/knmi[divisor]
        
        #when no rain or divisor = 0, will result in NA
        #this is not actually NA, we know there is no rain, so set to 0
        knmi.loc[knmi[col].isnull(), col] = 0 
        knmi.replace([-np.inf, np.inf, np.nan], 0, inplace = True)
        
        return knmi[['weather_station', col]]
    
    def _feature_weather_discretize(self, weather_column, num):
        #TODO: enforce op types 
        
        knmi = self.knmi
        
        #define what the new column is called
        col = 'feature_{0}_cut_{1}_cat'.format(weather_column, num)

        #create the discretized column
        knmi[col] = pd.cut(knmi[weather_column], num)
        
        return knmi[['weather_station', col]]
     
    def _feature_weather_x_hr_ago(self, weather_column, steps):
        '''
        assumes it gets the feature for a series of hectopuntens and returns one value
        name of the function becomes the method
        '''
        
        knmi = self.knmi
        
        t = knmi.groupby('weather_station').apply(
            lambda x: x[weather_column].rolling(steps).apply(lambda x: x[-steps])).reset_index().set_index('datetime')
        
        return t.rename(columns={weather_column:'feature_{0}_{1}_ago_num'.format(weather_column, steps)})
    
    def _feature_weather_x_hr_agg(self, weather_column, steps, agg_func):
        
        knmi = self.knmi
        
        t = knmi.groupby('weather_station').apply(
            lambda x: x[weather_column].rolling(steps).apply(
                lambda x: agg_func(x))).reset_index().set_index('datetime')
        
        return t.rename(columns={weather_column:'feature_{0}_{1}_{2}_num'.format(weather_column, steps, agg_func.__name__)})
    
    
    def feature_weather_last_event_grid(self, feature_grid):
        #TODO: enforce binary columns
        for col in feature_grid['columns']:
            feature_frame = self._feature_weather_last_event(col)
            self.feature_frames.append(feature_frame.reset_index())
         
    def feature_weather_rain_intensity_grid(self, feature_grid): 
        #TODO: enforce numeric
        #TODO: enforce correct structure for grid or figure out better input struct
        for i in range(0, len(feature_grid['columns'])):
            col = feature_grid['columns'][i]
            div = feature_grid['divisors'][i]
            feature_frame = self._feature_weather_rain_intensity(col, div)
            self.feature_frames.append(feature_frame.reset_index())
    
    def feature_weather_discretize_grid(self, feature_grid):
        for col in feature_grid['columns']:
            for num in feature_grid['nums']:
                    feature_frame = self._feature_weather_discretize(col, num)
                    self.feature_frames.append(feature_frame.reset_index())
                    
    def feature_weather_x_hr_ago_grid(self, feature_grid):
        for col in feature_grid['columns']:
            for hour in feature_grid['hours']:
                feature_frame = self._feature_weather_x_hr_ago(col, hour)
                self.feature_frames.append(feature_frame.reset_index())

    def feature_weather_x_hr_agg_grid(self, feature_grid):
        for col in feature_grid['columns']:
            for hour in feature_grid['hours']:
                for agg_func in feature_grid['agg_funcs']:
                    feature_frame = self._feature_weather_x_hr_agg(col, hour, agg_func)
                    self.feature_frames.append(feature_frame.reset_index())

 
    def _commit_frames(self):
        full_frame = reduce(lambda df1,df2: pd.merge(df1,df2,on=['datetime','weather_station']), self.feature_frames)
        self.full_frame = full_frame
        return full_frame
    
    def _write_to_db(self, write_to_schema_name, conn):
        
        # auto-generate the name of feature table by increasing the feature. 
        q = '''SELECT * FROM information_schema.tables WHERE table_schema = '{0}';
        '''.format(write_to_schema_name)
        
        existing_feature_tables = pd.read_sql(q, con=conn)
        if existing_feature_tables.shape[0]>0:
            existing_feature_tables['versions'] = existing_feature_tables.table_name.map(lambda x: int(x.split('_')[-1].replace('v','')))
            last_version = existing_feature_tables.versions.max()
            new_version = last_version + 1
        else:
            new_version = 1
    
        # Change this if you want to change the naming convention of things
        write_to_table_name = 'knmi_feature_master_v{0}'.format(new_version)
        
        
        pandas_to_db(self.full_frame, write_path='temp/t2.csv',
                      schema_name=write_to_schema_name,
                      table_name=write_to_table_name, conn=conn, drop_table=False)
    
    
def weather_driver(weather_table, feature_config, conn, write_to_schema_name, return_frame=False):
    factory = WeatherFeatureFactory('{0}'.format(weather_table), conn)
    for i in dir(factory):
        method = getattr(factory, i)
        ## Check if it's not an internal thing and is actually a method
        if not i.startswith('__') and not i.startswith('_') and hasattr(method, '__call__'):
            ## column you want to run the aggregation on.
            ## if column already exists in hectopunten - its a single column feature.
            if i in feature_config.keys():
                method(feature_config[i])
    
    f = factory._commit_frames()
    if return_frame == True:
        return f 
    factory._write_to_db(write_to_schema_name, conn=conn)   
