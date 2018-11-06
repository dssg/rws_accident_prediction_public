import pandas as pd
import sys
import os
from functools import reduce
from utils.misc_utils import pandas_to_db

class TemporalFeatureFactory(object):
    def __init__(self, time_granularity, start_date, end_date):
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
        
        self.times = pd.DataFrame(pd.date_range(start_date, end_date, freq=time_granularity), columns=['datetime'])
        self.feature_frames = [] # each feature function appends results here
        self.full_frame = None # feature_frames is squished together to make the full frame
        self.time_granularity = time_granularity

    ### 
    # DEFINE FEATURES THAT CAN BE CREATED
    ###
        
    def feature_time_weekend(self):
        # is this time a weekend, returns vector of 1s and 0s
        times = self.times
        times['weekend_cat'] = times['datetime'].map(lambda x: x.dayofweek)
        times['weekend_cat'] = ((times['weekend_cat'] == 5) | (times['weekend_cat'] == 6))
        self.feature_frames.append(times[['datetime', 'weekend_cat']])
    
    def feature_time_day_of_week(self):
        # what day of the week is it, returns vector of values 0 to 6
        times = self.times
        times['day_of_week_cat'] = times['datetime'].map(lambda x: x.dayofweek)
        self.feature_frames.append(times[['datetime', 'day_of_week_cat']])
    
    def feature_time_week_of_year(self):
        # what week of the year is it, returns vector of values 1 to 53
        times = self.times
        times['week_of_year_cat'] = times['datetime'].map(lambda x: x.weekofyear)
        self.feature_frames.append(times[['datetime', 'week_of_year_cat']])
    
    def feature_time_hour(self):
        # what hour of the day is it, returns vector of values 0 to 23
        times = self.times
        times['hour_cat'] = times['datetime'].map(lambda x: x.hour)
        self.feature_frames.append(times[['datetime', 'hour_cat']])
    
    def feature_time_month(self):
        # what month is it, returns vector of values 1 to 12
        times = self.times
        times['month_cat'] = times['datetime'].map(lambda x: x.month)
        self.feature_frames.append(times[['datetime', 'month_cat']])
    
    def feature_time_rushhour(self):
        # is this time during a weekday rush hour, returns vector of 0s and 1s
        times = self.times
        times['weekday_cat'] = times['datetime'].map(lambda x: x.dayofweek)
        times['weekday_cat'] = ((times['weekday_cat'] != 5) & (times['weekday_cat'] != 6))

        times['t'] = times['datetime'].map(lambda x: x.hour)
        times['rushhour_cat']  = ((times['t'] >= 7) & (times['t'] <= 9)) | ((times['t'] >= 15) & (times['t'] <= 19))

        times['rushhour_cat'] = times['rushhour_cat'] * times['weekday_cat']
        self.feature_frames.append(times[['datetime', 'rushhour_cat']])

    def feature_time_morn_rushhour(self):
        # is this time during a weekday morning rush hour, returns vector of 0s and 1s
        times = self.times
        times['weekday_cat'] = times['datetime'].map(lambda x: x.dayofweek)
        times['weekday_cat'] = ((times['weekday_cat'] != 5) & (times['weekday_cat'] != 6))

        times['t'] = times['datetime'].map(lambda x: x.hour)
        times['morn_rushhour_cat']  = ((times['t'] >= 7) & (times['t'] <= 9))
        times['morn_rushhour_cat'] = times['morn_rushhour_cat'] * times['weekday_cat']

        self.feature_frames.append(times[['datetime', 'morn_rushhour_cat']])

    def feature_time_eve_rushhour(self):
        # is this time during a evening morning rush hour, returns vector of 0s and 1s
        times = self.times
        times['weekday_cat'] = times['datetime'].map(lambda x: x.dayofweek)
        times['weekday_cat'] = ((times['weekday_cat'] != 5) & (times['weekday_cat'] != 6))

        times['t'] = times['datetime'].map(lambda x: x.hour)
        times['eve_rushhour_cat']  = ((times['t'] >= 15) & (times['t'] <= 19))
        times['eve_rushhour_cat'] = times['eve_rushhour_cat'] * times['weekday_cat']
        
        self.feature_frames.append(times[['datetime', 'eve_rushhour_cat']])
 
    
    def _commit_frames(self):
        full_frame = reduce(lambda df1,df2: pd.merge(df1,df2,on=['datetime']), self.feature_frames)
        self.full_frame = full_frame
        return full_frame
    
    def _write_to_db(self, write_to_schema_name, conn):
        
        # auto-generate the name of feature table by increasing the feature. 
        q = '''SELECT * FROM information_schema.tables WHERE table_schema = '{0}';
        '''.format(write_to_schema_name)
        
        existing_feature_tables = pd.read_sql(q, con=conn)
        existing_feature_tables = existing_feature_tables.loc[existing_feature_tables.table_name.str.contains(self.time_granularity)]
        if existing_feature_tables.shape[0]>0:
            existing_feature_tables['versions'] = existing_feature_tables.table_name.map(lambda x: int(x.split('_')[-1].replace('v','')))
            last_version = existing_feature_tables.versions.max()
            new_version = last_version + 1
        else:
            new_version = 1
    
        # Change this if you want to change the naming convention of things
        write_to_table_name = 'temporal_feature_{0}_v{1}'.format(self.time_granularity, new_version)
              
        pandas_to_db(self.full_frame, write_path='temp/t2.csv',
                      schema_name=write_to_schema_name,
                      table_name=write_to_table_name, conn=conn, drop_table=False)

def temporal_driver(time_granularity, start_date, end_date, conn, write_to_schema_name, return_frame=False):
    factory = TemporalFeatureFactory(time_granularity, start_date, end_date)
    
    # for every method in factory
    for i in dir(factory):
        method = getattr(factory, i)
        ## Check if it's not an internal thing and is actually a method
        if not i.startswith('__') and not i.startswith('_') and hasattr(method, '__call__'):
            # if it's not internal, run it
            method()
    
    #squish all the features together
    f = factory._commit_frames()
    
    # write to database
    factory._write_to_db(write_to_schema_name, conn=conn)   
        
    if return_frame == True:
        return f
