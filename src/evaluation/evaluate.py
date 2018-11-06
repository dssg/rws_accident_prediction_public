import os
from utils.read_exp_utils import read_raw_y, read_results_for_experiment, read_empty_results, read_experiment_result_db, read_raw_y_db
from utils.write_exp_utils import update_results_eval, update_raw_eval
import sys
import pandas as pd
import numpy as np
from sklearn.metrics import *
import itertools
from utils.misc_utils import connect_rds
from psycopg2.extras import Json, DictCursor

#ResultEvaluator:

def filter_time(df_raw_scores, time):
    '''Return dataframe filter by time'''
    df=df_raw_scores.copy()
    if time == 'night':
        df['hour'] = pd.to_datetime(df['time']).apply(lambda x: x.hour)
        df = df.loc[(df['hour'] > 19) | (df['hour'] < 6)]
    elif time == 'day':
        df['hour'] = pd.to_datetime(df['time']).apply(lambda x: x.hour)
        df = df.loc[(df['hour'] < 19) & (df['hour'] > 6)]
    return df

def tp_group(tp_group_df, k):
    """Function called to find the top k for each time group"""
    #take the top k from each group
    tp_group_df = tp_group_df.head(k)
    #return the total tp in each group
    return tp_group_df['y_true'].sum()

def pr_at_k(df, k):
    """
        Returns p/r for a specific result at a specific k
        df: pandas df with columns 'space', 'time', 'y_true', and 'y_pred'
        k: the number of obs you'd like to label 1 at each time
    """
    
    #static traits of df
    universe = df['time'].nunique()
    p = df['y_true'].sum()

    #needs to be sorted by (time, y_pred)
    #group by time and find the num tp in the top k
    tp = df.groupby('time').pipe(tp_group, k)
    fp = (universe*k) - tp

    precision = tp/(tp+fp)
    recall = tp/p

    return precision, recall

def pr_all_k(df):
    """
        Returns all p/r for a specific result_id
        df: pandas df with columns 'space', 'time', 'y_true' and 'y_pred'
    """

    space_universe = df['space'].nunique()
    precision_array = []
    recall_array = []

    for k in range(1, space_universe+1):
        precision, recall = pr_at_k(df, k)
        precision_array.append(precision)
        recall_array.append(recall)

    return precision_array, recall_array

class EvaluationResultUpdater:
    def __init__(self,experiment_ids):
        self.experiment_ids = experiment_ids
        print('Evaluation Result Updater is ready.\n call update result_tables to write results to the database')


    def _fill_result_table_for_experiment(self, df, result_id:int):

        df_day = filter_time(df, 'day')
        df_night = filter_time(df, 'night')

        p8, r8 = pr_at_k(df_day, 8)
        p1, r1 = pr_at_k(df_night, 1)

        update_results_eval('p_8_dayshift', p8, result_id)
        update_results_eval('r_8_dayshift', r8, result_id)
        update_results_eval('p_1_nightshift', p1, result_id)
        update_results_eval('r_1_nightshift', r1, result_id)


    def _fill_raw_table_for_experiment(self, df, result_id:int):

        df_day = filter_time(df, 'day')
        df_night = filter_time(df, 'night')

        precision_d, recall_d = pr_all_k(df_day)
        precision_n, recall_n = pr_all_k(df_night)
        precision_a, recall_a = pr_all_k(df)

        pr_dict = Json({'recall_day': recall_d, 'precision_day': precision_d,
                        'recall_night': recall_n, 'precision_night': precision_n,
                        'recall_all': recall_a, 'precision_all': precision_a})

        update_raw_eval(pr_dict, result_id)



    def update_result_tables(self):

        for experiment_id in self.experiment_ids:

            result_ids = read_results_for_experiment(experiment_id)   
 
            for result_id in result_ids: 

                df = read_raw_y(result_id)            
            
                print('updating result tables for experiment:',experiment_id)
                self._fill_result_table_for_experiment(df, result_id)
                self._fill_raw_table_for_experiment(df, result_id)

