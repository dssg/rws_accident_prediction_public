import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.utils.validation import check_X_y, check_array, check_is_fitted
from sklearn.utils.multiclass import unique_labels
import itertools
import datetime as dt
import calendar
import numpy as np

def period_of_day(hour):
    if hour <= 4 or hour >= 20:
        return "night"
    elif 5 <= hour <= 9:
        return "morning rush hour"
    elif 10 <= hour <= 14:
        return "afternoon"
    elif 15 <= hour <= 19:
        return "evening rush hour"
    else:
        print("error")
        return None
    
def create_time_hectokey_df(datetime_series, hectokey_merged):
    datetime = pd.to_datetime(datetime_series)
        
    day_of_week = datetime.dt.dayofweek
    is_weekend = np.where(day_of_week >= 5, True, False)
    month = datetime.dt.month.astype(int)
    year = datetime.dt.year.astype(int)
    hour = datetime.dt.hour.astype(int)
    
    #create a time feature matrix
    X_dict = {'hectokey_merged':hectokey_merged,
              'hour':hour,
             'is_weekend':is_weekend,
             'month':month,
             'year':year}
    X = pd.DataFrame(X_dict)
    X['time_of_day'] = X['hour'].apply(period_of_day)
    return X
    
def num_hours_per_period(time_of_day):
    hours_list = list(map(period_of_day, list(range(0,24))))
    return hours_list.count(time_of_day)

def count_hours_in_range(years_range, month, time_of_day, is_weekend, granularity_min):
    '''Returns the number of days in a range of dates'''
    weekday_mask = np.zeros(7, dtype='int')
    if is_weekend:
        weekday_mask[5:] = 1
    else:
        weekday_mask[:5] = 1
    weekday_mask = weekday_mask.tolist()
    
    count_days = 0
    for y in range(years_range[0], years_range[1]+1):
        #we will average over one month
        #for m in range(month_range[0], month_range[1]+1):
        m = month
        start = dt.date(y, m, 1)
        end = dt.date(y, m, calendar.monthrange(y, m)[1]) 
        end = end + dt.timedelta(days=1)
        days = np.busday_count(start, end, weekmask=weekday_mask)
        count_days += days
    return num_hours_per_period(time_of_day) * count_days * (60/granularity_min)

def set_accident_probability(row, X, y, years_range, time_granularity_min):
    hectokey_merged = row['hectokey_merged']
    month = int(row['month'])
    time_period = row['time_of_day']
    is_weekend = row['is_weekend']
    total_hours = count_hours_in_range(years_range, month, time_period, is_weekend, time_granularity_min)
    
    if total_hours==0:
        print(row)
    
    total_accidents = y[(X['hectokey_merged'] == hectokey_merged)
                 & (X['time_of_day'] == time_period)
                 & (X['is_weekend'] == is_weekend)
                 & (X['month'] == month)].sum()
    

    return total_accidents/total_hours

def find_accident_probability(row, df):
    
    return df[(df['hectokey_merged'] == row['hectokey_merged']) 
              & (df['is_weekend'] == row['is_weekend'])
              & (df['time_of_day'] == row['time_of_day'])
              & (df['month'] == row['month'])]['Accidents_Prediction'].sum()
    

class BL_smart(BaseEstimator, ClassifierMixin):

    def __init__(self, time_granularity_min=None, X_datetime_train=None, X_hectokey_merged_train=None, X_datetime_val=None, X_hectokey_merged_val=None):
        self.time_granularity_min = time_granularity_min
        self.X_datetime_train = X_datetime_train
        self.X_hectokey_merged_train = X_hectokey_merged_train
        self.X_datetime_val = X_datetime_val
        self.X_hectokey_merged_val = X_hectokey_merged_val

    def fit(self, X, y):

        # Check that X and y have correct shape
        #X, y = check_X_y(X, y)
        # Store the classes seen during fit
        #self.classes_ = unique_labels(y)

        self.X_ = X
        self.y_ = y
        
        # get time feature matrix
        X_time = create_time_hectokey_df(self.X_datetime_train, self.X_hectokey_merged_train)
        
        # create a unque row for every combination of hectokey_merged, datetime_rounded, weekday_or_weekend, hour_interval
        features_dict = {"hectokey_merged" : X_time['hectokey_merged'].unique(),
                "is_weekend" : X_time['is_weekend'].unique(),
                 "month" : X_time['month'].unique(),
                "time_of_day" : ['morning rush hour', 'afternoon', 'evening rush hour','night']}
        df = pd.DataFrame(list(itertools.product(*features_dict.values())), columns=features_dict.keys())
        
        # get the total years covered in the dataset
        years_range =X_time['year'].unique()
        print (years_range)
        if(years_range.shape[0]) == 1:
            years_range = [years_range[0], years_range[0]]
        else:
            years_range = [years_range.min(), years_range.max()]
            

        # For each combination of hectokey and temporal features, 
        # get total number of accidents occuring in that segment on that type of day, 
        # and divide by total number of hours to get probability of accident occuring
        df['Accidents_Prediction'] = df.apply(lambda row: set_accident_probability(row, X_time, y, years_range, self.time_granularity_min), axis = 1)
    
        self.years_range = years_range
        self.df = df

        return self

    def predict_proba(self, X):
    
        # Check is fit had been called
        check_is_fitted(self, ['X_', 'y_'])

        # Input validation
        #X = check_array(X)

        # generate time features
        X_val = create_time_hectokey_df(self.X_datetime_val, self.X_hectokey_merged_val)
        
        pred_prob1 = X_val.apply(lambda row: find_accident_probability(row, self.df), axis = 1)

        return np.c_[1-pred_prob1,pred_prob1]

    
    
class BL_random(BaseEstimator, ClassifierMixin):

    def __init__(self, random_state=None):
        self.random_state = random_state

    def fit(self, X, y):

       
        self.X_ = X
        self.y_ = y
       
        return self

    def predict_proba(self, X):
    
        # Check is fit had been called
        check_is_fitted(self, ['X_', 'y_'])

        # Input validation
        #X = check_array(X)

        # generate time features
        pd.np.random.seed(self.random_state)
        
        pred_prob1 = pd.Series([pd.np.random.random() for i in range(0, X.shape[0])])

        return np.c_[1-pred_prob1,pred_prob1]
    