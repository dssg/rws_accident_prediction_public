"""
Includes scripts which orchestra_utils need
"""

import pandas as pd
from itertools import product
import sklearn

def return_timed_spine(df, starttime, endtime):
    '''
    assumes spine has datetime_rounded as a column
    '''
    return df.loc[((df.datetime_rounded<=endtime) & (df.datetime_rounded>=starttime)),:]



def time_mark_generator(start_date, end_date, ratio):
    total = pd.date_range(start_date, end_date, freq='60min')
    ratio_date_index = int(ratio*total.shape[0])
    return total[ratio_date_index]


def sampler(conn, spine, start_date, end_date, merged_hectopunten_name, 
            sampling_method, positive_perc, time_granularity, seed, write=0):
    '''
    spine_table_name, str
    the spine as a df, containing three columns for all positive samples: datetime_rounded, hectokey_merged, and accidents
    
    start_date, str
    'YYYY-MM-DD' start date of data collection for positive samples
    
    end_date, str
    'YYYY-MM-DD' end date of data collection for positive samples
    not inclusive!!!!!!!!
    
    sampling_method, str
    the method by which we negative sample, defaults to random
    accepts 'random' and '' ???
    
    unique_hectopunten_name, str
    the schema.table that includes a list of merged_hectokey s
    '''
    #TODO: make sure that sampling method is supported
    #TODO: make sure that merged_hectopunten_name exists
    #TODO: make sure that positive_perc is between 0 and 1
    #TODO: build other sampling methods
    #TODO: make the if write thing actually work when Harsh pushes the function
    #TODO: filter input data by the start_date and end_date
    
    #convert spine to only include relevant columns and rows
    spine = spine[['hectokey_merged', 'datetime_rounded', 'accidents']]
    
    #find the time and space universe from which to randomly sample
    time_universe = pd.date_range(start_date, end_date, freq=time_granularity)
    hecto_universe_query = "select distinct hectokey_merged from {0};".format(merged_hectopunten_name) 
    hecto_universe = pd.read_sql(hecto_universe_query, con=conn)['hectokey_merged']
    
    #find how many negative samples to take
    positive_len = spine.shape[0]
    negative_num = int((positive_len/positive_perc) - positive_len)
    
    
    
    
    #create df to fill
    negative_df = pd.DataFrame(columns=['hectokey_merged', 'datetime_rounded', 'accidents'])
    
    #create df for positive samples with correct columns
    positive_samples = spine[['hectokey_merged', 'datetime_rounded']]
    
    if sampling_method == 'full':
        cross = list(product(hecto_universe, time_universe))
        cross = pd.DataFrame(cross, columns = ['hectokey_merged', 'datetime_rounded'])
        
        #create spine to check against
        spine_check = spine[['hectokey_merged', 'datetime_rounded']]
        
        #find which obs in cross are not in spine
        negative_cross = pd.concat([cross, spine_check]).drop_duplicates(keep=False).assign(accidents=0)
        
        #combine positive and negative sample to create full data
        full_sample = pd.concat([negative_cross, spine], ignore_index=True)
        
        return full_sample
        
    elif sampling_method == 'random':
        negative_df_list = []
        
        cross = list(product(hecto_universe, time_universe))
        cross = pd.DataFrame(cross, columns = ['hectokey_merged', 'datetime_rounded'])
        
        # This only keeps the pairs which belond in cross
        cross_minus_spine = pd.merge(cross, spine[['hectokey_merged', 'datetime_rounded']],
                                     indicator=True, how='outer').query(
            '_merge=="left_only"').drop('_merge', axis=1)
        
        negative_df = cross_minus_spine.sample(negative_num, random_state = seed)
        negative_df["accidents"] = 0
        
    #combine positive and negative sample to create full data
    full_sample = pd.concat([negative_df, spine], ignore_index=True)
    
    if write:
        return full_sample
    else:    
        return full_sample

    
def generate_temporal_features(frame, date_col = 'datetime_rounded'):
    '''
    Takes the panel and for date_col generates datetime features
    #todo! days since 2016 to be added
    '''
    
    
    frame['day_of_week'] = frame[date_col].map(lambda x: x.dayofweek)
    frame['week_of_year'] = frame[date_col].map(lambda x: x.weekofyear)
    frame['hour'] = frame[date_col].map(lambda x: x.hour)
    frame['month'] = frame[date_col].map(lambda x: x.month)
    
    return frame


def preprocessor(df_train, df_val, df_test, label, 
               numerical_cols,
               categorical_cols, ignore_test=False):
    """
    Preparing data for sklearn models.
    
    Processing steps:
     - Converting problem into a classification problem
     - Imputing the numerical data with means of train
     - StandardScaling the numerical data
     - Dummy Variables for the categorical data
     - Keeping on columns which exist in train, over test and val
    
    
    dfs - the dataset containing all data for model
    label - what is the column name for the y variable
    categorical_cols - categorical columns that need to be converted to binaries
    categorical_counts - the number of categories in each of the mentioned types
    
    """
    #TODO: figure out how to identify numeric columns
    #TODO: deal with categorical variables
    
#     for col in numerical_cols:
#         print(col)
#         df_train.loc[:,col] = pd.to_numeric(df_train[col])
    print('Casting to Numeric - Train')
    df_train.loc[:,numerical_cols] = df_train.loc[:, numerical_cols].apply(pd.to_numeric, errors='coerce')
    
    print('Casting to Numeric - Val')
    df_val.loc[:,numerical_cols] = df_val.loc[:, numerical_cols].apply(pd.to_numeric, errors='coerce')
    
    
    if not ignore_test:
        for col in numerical_cols:
            df_test.loc[:,col] = pd.to_numeric(df_test[col])

    #seperate x and y by using the label
    X_train = df_train.loc[:, df_train.columns != label].copy()
    y_train = df_train.loc[:, label].copy()
    if not ignore_test:
        X_test = df_test.loc[:, df_test.columns != label].copy()
        y_test = df_test.loc[:, label].copy()
    X_val = df_val.loc[:, df_val.columns != label].copy()
    y_val = df_val.loc[:, label].copy()
    
    # We are losing information here, we could decide to repeat these rows instead.
    #make sure y is a binary
    y_train[y_train > 1] = 1    
    if not ignore_test:
        y_test[y_test > 1] = 1  
    y_val[y_val > 1] = 1  
    

    #X_train.to_csv('X_train.csv')
    #X_val.to_csv('X_val.csv')
    #y_train.to_csv('y_train.csv')
    #y_val.to_csv('y_val.csv')


    #fill na's with mean of train set
    print('NUMERICAL COLS: ',len(numerical_cols))
    means = X_train.loc[:, numerical_cols].mean()
    #means.to_csv('X_train_means.csv')
 
    # print(means.locl[pd.isnull(means])

    X_train.fillna(means, inplace=True)
    if not ignore_test:
        X_test.loc[:, numerical_cols] = X_test.loc[:, numerical_cols].fillna(means)
    X_val.fillna(means, inplace=True)
    
    t = X_train.isnull().sum()
#     t.to_csv('X_train_nulls.csv')
#     X_train.to_csv('X_train.csv')
    t = t.loc[t==X_train.shape[0]]
    print(t)    


    #train numerical column scaler based off the x_train data
    print("Scaling: ")
    scaler = sklearn.preprocessing.StandardScaler()
    scaler = scaler.fit(X_train.loc[:,numerical_cols])
    
    #apply scaler to all data
    X_train_num = pd.DataFrame(scaler.transform(X_train.loc[:,numerical_cols]), 
                               columns = numerical_cols, index = X_train.index.values)
    
    if not ignore_test:
        X_test_num = pd.DataFrame(scaler.transform(X_test.loc[:,numerical_cols]), 
                              columns = numerical_cols, index =  X_test.index.values)
    X_val_num = pd.DataFrame(scaler.transform(X_val.loc[:,numerical_cols]), 
                             columns = numerical_cols, index =  X_val.index.values)
    
    #create dummy variables for categorical variables in each dataset
    X_train_temp = pd.get_dummies(X_train[categorical_cols], columns=categorical_cols, dummy_na=True)
    X_val_temp = pd.get_dummies(X_val[categorical_cols], columns=categorical_cols, dummy_na=True) 
    if not ignore_test:
        X_test_temp = pd.get_dummies(X_test[categorical_cols], columns=categorical_cols, dummy_na=True)
    
    #find what dummy variables exist in the X_train set
    required_cols = X_train_temp.columns
    X_val_temp_cols = X_val_temp.columns
    if not ignore_test:
        X_test_temp_cols = X_test_temp.columns    
    
    for col in required_cols:
    	#if a column is not in the val or test, add it and set all to 0
        if col not in X_val_temp_cols:
            X_val_temp.loc[:,col] = 0
        if not ignore_test:
            if col not in X_test_temp_cols:
                X_test_temp.loc[:,col] = 0 

    #select only the dummy cols that are in train for val and test 
    if not ignore_test:
        X_test_temp = X_test_temp.loc[:,required_cols]
    X_val_temp = X_val_temp.loc[:,required_cols]

    #combine categorical columns with numeric columns for each set
    if not ignore_test:
        X_test = pd.concat([X_test_num, X_test_temp], axis = 1)
    X_val = pd.concat([X_val_num, X_val_temp], axis = 1)
    X_train = pd.concat([X_train_num, X_train_temp], axis = 1) 
    
    #return X and y for train, val and test
    if not ignore_test:
        return X_train, y_train,  X_val, y_val, X_test, y_test
    else:
        return X_train, y_train, X_val, y_val

def rinse_spines(frame):
    '''
    rinses the spines of columns that are no longer needed such as:
     - datetimes
     - 
    '''
    
    #Note: Potential Bug Situation if any of the features are named as here. 

    unwanted_columns = ['datetime_rounded', 'datetime_rounded_lag', 'datetime_rounded_lag_rounded', 'datetime_rounded_lag_rounded_flow', 'datetime_rounded_lag_rounded_weather', 'datetime_rounded_lag_rounded_ongevallen',
     'datetime_rounded_year_minus_one','weather_station','datetime','year', 'end_datetime']
    needed_columns = list(set(frame.columns)-set(unwanted_columns))
    
    return frame.loc[:,needed_columns]


import pandas as pd
def temporal_split(label_start, label_end, minimum_gap_size,
                    rolling_type, minimum_train_size, minimum_val_size):

    '''
    label_start: pandas Timestamp
        Start of labels
    label_end: pandas Timestamp
        End of label
    minimum_gap_size: pandas DateOffset
        Offset gap between end of train and start of val
    rolling_type: text
        method of rolling, default:'sliding'
    minimum_train_size: pandas DateOffset
        The minimum size of train set.
    minimum_val_size: pandas DateOffset
        The minimum size of val set
    '''
    
    #TODO! Fix Bug: Missing one month in the end because of 1st-31st thing. 
    
    minimum_gap_size = pd.offsets.DateOffset(**minimum_gap_size)
    minimum_train_size = pd.offsets.DateOffset(**minimum_train_size)
    minimum_val_size = pd.offsets.DateOffset(**minimum_val_size)
    
    train_window_start = label_start
    train_window_end = train_window_start + minimum_train_size
        
    
    flag = True
    while flag:
        val_window_start = train_window_end + minimum_gap_size
        val_window_end = val_window_start + minimum_val_size
        
        
        if val_window_end > label_end:
            flag = False
        else:
        
            split = {
                'train_start': train_window_start,
                'train_end': train_window_end,
                'val_start': val_window_start,
                'val_end': val_window_end,
            }
        
            if rolling_type=='window':
                train_window_start = train_window_start + minimum_val_size #assuming this is same as model update frequency
                train_window_end = train_window_start + minimum_train_size
        
            elif rolling_type=="expanding":
                train_window_start = train_window_start
                train_window_end = train_window_end + minimum_val_size
            yield split
    
