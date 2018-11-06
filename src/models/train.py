from xgboost import XGBClassifier
from sklearn.model_selection import ParameterGrid
from sklearn.pipeline import Pipeline
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit, RandomizedLogisticRegression
from sklearn.neighbors.nearest_centroid import NearestCentroid
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import LinearSVC, SVC
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import *
import numpy as np
from utils import read_exp_utils
from utils.write_exp_utils import ResultConfig, create_parameter_id
from utils.misc_utils import calculate_accident_pct
from xgboost import XGBClassifier

from datetime import datetime
from utils.orchestra_utils import  preprocessor, sampler, rinse_spines, temporal_split, return_timed_spine, generate_temporal_features

from models.BaselineClassifier import BL_smart, BL_random

CLASSIFIER_CONFIG = {
    'XG': XGBClassifier(),
    'RF': RandomForestClassifier(),
    #'LR': LogisticRegression(),
    #'KNN': KNeighborsClassifier(n_neighbors=3),
    #'NB': GaussianNB(),
    #'ET': ExtraTreesClassifier(),
   # 'AB': AdaBoostClassifier(),
    'SVM': SVC(probability=True), 
    #'GB': GradientBoostingClassifier(),
 #   'DT': DecisionTreeClassifier(), 
#    'SGD': SGDClassifier(),
    'BASE_SMART': BL_smart(),
    'BASE_RANDOM': BL_random()
}


import pandas as pd

def load_experiment(experiment_id:int):
    out = read_exp_utils.read_experiment(experiment_id) 
    classifier_config = out[0]
    grid_config = out[1]
    return classifier_config, grid_config 

############## PRECISION AT K
def joint_sort_descending(l1, l2):
    # l1 and l2 have to be numpy arrays
    idx = np.argsort(l1)[::-1]
    return l1[idx], l2[idx]


def generate_binary_at_k(y_scores, k):
    cutoff_index = int(len(y_scores) * (k / 100.0))
    test_predictions_binary = [1 if x < cutoff_index else 0 for x in range(len(y_scores))]
    return test_predictions_binary


def precision_at_k(y_true, y_scores, k):
    y_scores, y_true = joint_sort_descending(np.array(y_scores), np.array(y_true))
    preds_at_k = generate_binary_at_k(y_scores, k)
    #precision, _, _, _ = metrics.precision_recall_fscore_support(y_true, preds_at_k)
    #precision = precision[1]  # only interested in precision for label 1
    precision = precision_score(y_true, preds_at_k)
    return precision




from sklearn.metrics import *

from utils.write_exp_utils import ResultConfig

import pickle


def adjusted_pred(y_pred_probs, t):
    """
    Return predictions based on a given threshold(t)
    """
    return [1 if y >= t else 0 for y in y_pred_probs]

def precision_recall_thresholds(y_true, y_pred_probs,threshold_step=0.01):
    precision_list = list()
    recall_list = list()
    thresholds=np.arange(0.0, 1.0, threshold_step)
    for t in thresholds:
        y_pred = adjusted_pred(y_pred_probs,t)
        precision_list.append(precision_score(y_true,y_pred,pos_label=1))
        recall_list.append(recall_score(y_true,y_pred,pos_label=1))
    #Recreate output in http://scikit-learn.org/stable/modules/generated/sklearn.metrics.precision_recall_curve.html#sklearn.metrics.precision_recall_curve
    precision_list.append(1.0)
    recall_list.append(0.0)
    return precision_list, recall_list, thresholds




def train_loop(spine_label,

               spine_test,

               experiment,
               conn
              ):
    
    
    print(spine_label.columns)
    
    grid_parameters = experiment.GRID_PARAMETERS
    classifiers = experiment.CLASSIFIERS
    experiment_id = experiment.EXPERIMENT_ID
    random_state = experiment.RANDOM_SEED
    
    print("""
    ####################
    ## NEGATIVE SAMPLING (TEST)
    ####################""")
    
    
    print(spine_test.columns)
    
    if experiment.SAMPLE_TEST:
            print('Starting with Sampling for Test')
            spine_test = sampler(spine=spine_test, conn=conn, start_date=experiment.TEST_LABEL_START, seed=experiment.RANDOM_SEED,
                                  end_date=experiment.TEST_LABEL_END, **experiment.TEST_SAMPLE_METHOD_PARAMS)

            
            
    minimum_gap_size = experiment.VALIDATION_CONFIG['minimum_gap_size']
    rolling_type = experiment.VALIDATION_CONFIG['rolling_type']
    minimum_train_size = experiment.VALIDATION_CONFIG['minimum_train_size']
    minimum_val_size = experiment.VALIDATION_CONFIG['minimum_val_size']
           
    
    q = 'select end_datetime, hectokey_merged, {0}  from {1}'.format(','.join(experiment.FEATURES_FLOW_LIST), experiment.FEATURES_FLOW_TABLE )
    
    
    print('Loading flow table')                  
    flow_feature_table = pd.read_sql('select end_datetime, hectokey_merged, {0}  from {1}'.format(
             ','.join(experiment.FEATURES_FLOW_LIST),experiment.FEATURES_FLOW_TABLE), con=conn)
   
    
    print('Loading Ongevallen Data')
    ongevallen_feature_table = pd.read_sql('select datetime, hectokey_merged, {0} from {1}'.format(','.join(experiment.FEATURES_ONGEVALLEN_LIST),
                                                                                     experiment.FEATURES_ONGEVALLEN_TABLE), con=conn)
        
    print('Loading Weather Feature Data')
    weather_station = pd.read_sql('select * from {0}'.format(experiment.WEATHER_STATION_MAPPING_TABLE), con=conn)
    knmi_feature_table = pd.read_sql('select datetime, weather_station, {0}  from {1}'.format(
            ','.join(experiment.FEATURES_KNMI_LIST),experiment.FEATURES_KNMI_TABLE), con=conn)
    
    print('Loading Hectopunten Feature Data')
    hectopunten_feature_table = pd.read_sql('select rollup_year, hectokey_merged, {0}  from {1}'.format(
            ','.join(experiment.FEATURES_HECTOPUNTEN_LIST),experiment.FEATURES_HECTOPUNTEN_TABLE), con=conn)
    
    
    print('Loading Temporal Feature Data')
    temporal_feature_table = pd.read_sql('select datetime, {0} from {1}'.format(','.join(experiment.FEATURES_TEMPORAL_LIST), experiment.FEATURES_TEMPORAL_TABLE), con=conn)
    
    print('Starting splits') 
    for split_num, split in enumerate(temporal_split(experiment.LABEL_START, experiment.LABEL_END, minimum_gap_size,
                    rolling_type, minimum_train_size, minimum_val_size)):
        

        print(split)

        # Do the processing of the splits here. 
        spine_train_local = return_timed_spine(spine_label, split['train_start'], split['train_end'])
        spine_val_local = return_timed_spine(spine_label, split['val_start'], split['val_end'])
        #Calcuate accident percentage 
        pre_sample_train_local = spine_train_local
        experiment.TIME_GRANULARITY=60 ####REPLACE THIS LINE ONCE NEW experiment table is ready
        accident_pct_train_pre_sample=calculate_accident_pct(experiment.SEGMENTATION_TABLE,experiment.TIME_GRANULARITY,split['train_start'],split['train_end'],pre_sample_train_local)
        print("""
    ####################
    ## NEGATIVE SAMPLING (TRAIN/VAL)
    ####################""")
        
        #Sampling
        if experiment.SAMPLE_TRAIN:
            print('Starting with Sampling for Train')
            print('Original Shape:', spine_train_local.shape)
            spine_train_local = sampler(conn=conn, spine=spine_train_local, start_date=split['train_start'], seed=experiment.RANDOM_SEED, end_date=split['train_end'], **experiment.TRAIN_SAMPLE_METHOD_PARAMS)
            
            print(spine_train_local.columns)

        if experiment.SAMPLE_VAL:
            print('Starting with Sampling for Val')
            spine_val_local = sampler(conn=conn, spine=spine_val_local, start_date=split['val_start'], seed=experiment.RANDOM_SEED,
                              end_date=split['val_end'], **experiment.VAL_SAMPLE_METHOD_PARAMS)
        
        
        
        
        print('''
    ####################
    ## GENERATING LAG AND LAG_ROUNDED
    ####################''')

        timedelta = pd.Timedelta(value=experiment.TIME_LAG_MINUTES, unit='m')

        spine_train_local.loc[:,"datetime_rounded_lag"] = spine_train_local.datetime_rounded - timedelta
        spine_val_local.loc[:,"datetime_rounded_lag"] = spine_val_local.datetime_rounded - timedelta


        #### DATETIME LAG ROUNDED FOR JOINING TO KNMI
        #60m is hard-coded here KNMI granularity is hardcoded.
        spine_train_local.loc[:,"datetime_rounded_lag_rounded_weather"] = spine_train_local.loc[:,'datetime_rounded_lag'].dt.floor('60min')
        spine_val_local.loc[:,"datetime_rounded_lag_rounded_weather"] = spine_val_local.loc[:,'datetime_rounded_lag'].dt.floor('60min')

        
        #### DATETIME LAG ROUNDED FOR JOINING FLOW
        spine_train_local.loc[:,"datetime_rounded_lag_rounded_flow"] = spine_train_local.loc[:,'datetime_rounded_lag'].dt.floor('{0}min'.format(experiment.FEATURES_FLOW_CONFIG['time_granularity_min']))
        spine_val_local.loc[:,"datetime_rounded_lag_rounded_flow"] = spine_val_local.loc[:,'datetime_rounded_lag'].dt.floor('{0}min'.format(experiment.FEATURES_FLOW_CONFIG['time_granularity_min']))

        
        #### YEAR LAG FOR JOINING HECTOPUNTEN
        spine_train_local.loc[:,"datetime_rounded_year_minus_one"] = spine_train_local.datetime_rounded.map(lambda x: x.year - 1)
        spine_val_local.loc[:,"datetime_rounded_year_minus_one"] = spine_val_local.datetime_rounded.map(lambda x: x.year - 1)
        
        #### DATETIME LAG ROUNDED FOR JOINING ONGEVALLEN
        spine_train_local.loc[:,"datetime_rounded_lag_rounded_ongevallen"] = spine_train_local.loc[:,'datetime_rounded_lag'].dt.floor('{0}min'.format(experiment.FEATURES_ONGEVALLEN_CONFIG['time_granularity_min']))
        spine_val_local.loc[:,"datetime_rounded_lag_rounded_ongevallen"] = spine_val_local.loc[:,'datetime_rounded_lag'].dt.floor('{0}min'.format(experiment.FEATURES_ONGEVALLEN_CONFIG['time_granularity_min']))


        print('Spine Train: ')
        print(spine_train_local.shape)
        print('Spine Val ')
        print(spine_val_local.shape)

        
        
        
        print('''
        ####################
        ## JOINING FEATURES
        ####################''')

 
        print('''
        ####################
        ### KNMI
        ####################''')

        

        spine_train_local = pd.merge(spine_train_local, weather_station, on='hectokey_merged', how='left')
        spine_val_local = pd.merge(spine_val_local, weather_station, on='hectokey_merged', how='left')

        

        spine_train_local = pd.merge(spine_train_local, knmi_feature_table, 
                               left_on=['datetime_rounded_lag_rounded_weather','weather_station'],
                               right_on=['datetime', 'weather_station'], how='left')

        spine_val_local = pd.merge(spine_val_local, knmi_feature_table,
                              left_on=['datetime_rounded_lag_rounded_weather','weather_station'],
                              right_on=['datetime','weather_station'], how='left')

#         print('Spine Label: ')
#         print(spine_label.shape)
#         print('Spine Test: ')
#         print(spine_test.shape)


        print('''
        #################
        ### HECTOPUNTENT
        #################''')

        

        spine_train_local = pd.merge(spine_train_local, hectopunten_feature_table,
                               left_on=['datetime_rounded_year_minus_one','hectokey_merged'],
                               right_on=['rollup_year','hectokey_merged'], how='left')

        spine_val_local = pd.merge(spine_val_local, hectopunten_feature_table,
                               left_on=['datetime_rounded_year_minus_one','hectokey_merged'],
                               right_on=['rollup_year','hectokey_merged'], how='left')



#         print('Spine Label: ')
#         print(spine_label.shape)
#         print('Spine Test: ')
#         print(spine_test.shape)

        print('''
        ########################
        ### JOINING FLOW FEATURES
        ########################''')


        spine_train_local = pd.merge(spine_train_local, flow_feature_table,
                               left_on=['datetime_rounded_lag_rounded_flow','hectokey_merged'],
                               right_on=['end_datetime','hectokey_merged'], how='left')

        spine_val_local = pd.merge(spine_val_local, flow_feature_table,
                               left_on=['datetime_rounded_lag_rounded_flow','hectokey_merged'],
                               right_on=['end_datetime','hectokey_merged'], how='left')

    #     del(flow_feature_table)

        print('Spine Label: ')
        print(spine_train_local.shape)
        print('Spine Test: ')
        print(spine_val_local.shape)


        print('''
        ###############################
        #### GENERATE TEMPORAL FEATURES
        ###############################''')
        
        spine_train_local = pd.merge(spine_train_local, temporal_feature_table, left_on='datetime_rounded', right_on='datetime', how='left')
        spine_val_local = pd.merge(spine_val_local, temporal_feature_table, left_on='datetime_rounded', right_on='datetime', how='left')


        print('Spine Label: ')
        print(spine_train_local.shape)
        print('Spine Test: ')
        print(spine_val_local.shape)

        
        print('''
        ###############################
        #### JOINING ONGEVALLEN FEATURES
        ###############################''')
        
        
        spine_train_local = pd.merge(spine_train_local, ongevallen_feature_table, left_on=['datetime_rounded_lag_rounded_ongevallen', 'hectokey_merged'],
                                     right_on=['datetime', 'hectokey_merged'], how='left')
        
        spine_val_local = pd.merge(spine_val_local, ongevallen_feature_table, left_on=['datetime_rounded_lag_rounded_ongevallen', 'hectokey_merged'],
                                     right_on=['datetime', 'hectokey_merged'], how='left')
        
        
        
        space_time = {'space':spine_val_local['hectokey_merged'].tolist(), 'time':[str(dtr) for dtr in spine_val_local['datetime_rounded'].tolist()]}
        
        
        hectokey_merged_train = spine_train_local.hectokey_merged
        datetime_train = spine_train_local.datetime_rounded
        
        hectokey_merged_val = spine_val_local.hectokey_merged
        datetime_val = spine_val_local.datetime_rounded
        
        
        print('''
        #######################################
        #### SPINE RINSING OF UNWANTED COLUMNS
        #######################################''')

       

        spine_train_local = rinse_spines(spine_train_local)
        spine_val_local = rinse_spines(spine_val_local)
    #     spine_test_local = rinse_spines(spine_test)


        
        #TODO! spine test doesn't need to be pre-process time and again. But for now let it be here.
        X_train, y_train, X_val, y_val = preprocessor(spine_train_local, spine_val_local, spine_test,
                                                            label='accidents',
                                                            numerical_cols=experiment.NUMERICAL_FEATURES_LIST,
                                                            categorical_cols=experiment.CATEGORICAL_FEATURES_LIST,
                                                     ignore_test=True)
    
        accident_pct_val = y_val.sum()/y_val.count()           
        print("FINAL SPLIT SIZES - TRAIN", X_train.shape)
        print("FINAL SPLIT SIZES - VAL:", X_val.shape)        
        for classifier in classifiers:

            clf = CLASSIFIER_CONFIG[classifier]
            parameter_values = grid_parameters[classifier]
            for p in ParameterGrid(parameter_values):
                #set hyperparameters and train the model
                p['random_state'] = random_state.item()
                
                # Not all classifiers can use all cores
                if classifier in ['RF', 'KNN']:
                    p['n_jobs'] = -1
               
                
                
                if classifier == 'BASE_SMART':
                    clf.set_params(
                        **{
                            'time_granularity_min':experiment.TIME_GRANULARITY,
                            'X_datetime_train':datetime_train,
                            'X_hectokey_merged_train':hectokey_merged_train,
                            'X_datetime_val':datetime_val,
                            'X_hectokey_merged_val':hectokey_merged_val
                        }
                    )
                else:
                    print(p)
                    clf.set_params(**p)
                
                if classifier == 'BASE_SMART':
                    parameters_id = 'Baseline'
                else:
                    parameters_id = create_parameter_id(p)
                print(parameters_id)

                #If XGBoost: convert dataframe to DMatrix
                if classifier == 'XG':
                    eval_set = [(X_val, y_val)]
                    clf.fit(X_train, y_train,early_stopping_rounds=10,  eval_metric="logloss", eval_set=eval_set, verbose=True)
                else:
                    clf.fit(X_train,y_train)     
                
                y_pred_probs = clf.predict_proba(X_val)[:,1]
                #AUC
                auc_roc= roc_auc_score(y_val, y_pred_probs)

                    #ROC

                fpr,tpr,thresholds =roc_curve(y_val, y_pred_probs, pos_label=1)

                roc = {'fpr':fpr.tolist(),'tpr':tpr.tolist(),'thresholds':thresholds.tolist()} # this should be a list of roc of each val set

                #PRC

                precision_list, recall_list, thresholds  = precision_recall_thresholds(y_val, y_pred_probs)

                prc = {'precision':precision_list,'recall':recall_list,'thresholds':thresholds.tolist()} # this should be a list of prc of each val set  

                #PICKLE

                #pickle_path_folder = "../../pickle_jar/"

                #pickle_path =  pickle_path_folder+str(datetime.now())+classifier+str(experiment_id)+str(split_num)+".p"

                #pickle_path = pickle_path.replace(" ", "")
                pickle_path="no pickle file"
                # pickle.dump( clf, open( pickle_path, "wb" ) )

                split_for_db = split.copy()
                
                
                split_for_db['train_start'] = str(split_for_db['train_start'])
                split_for_db['train_end'] = str(split_for_db['train_end'])
                split_for_db['val_start'] = str(split_for_db['val_start'])
                split_for_db['val_end'] = str(split_for_db['val_end'])
    
                ####record experimental result and results on database

                result = {'experiment_id':experiment_id,
                          
                          'val_set_id': split_num,

                         'model':classifier,

                         'parameters_id':parameters_id,

                         'parameters':p,
                          
                         'split_info':split_for_db,
                         
                         'train_size':X_train.shape[0],
                          
                         'val_size': X_val.shape[0],

                         'actual_features_size':X_train.shape[1],

                         'accident_pct_train_pre_sample':accident_pct_train_pre_sample,
                         
                         'accident_pct_val':y_val.sum()/y_val.count(), 

                         'p_8_dayshift':None,

                         'p_1_nightshift':None,
 
                         'r_8_dayshift':None,

                         'r_1_nightshift':None,
        
                         'auc_roc': auc_roc,

                         'pickle_path':pickle_path, # as of now single pkl

                         'remarks':None,

                         ####raw_y_score table 
                        'space_time': space_time, 
                         
                         'y_true':y_val.tolist(), # store y_true and y_scores in a separate table

                         'y_scores':y_pred_probs.tolist(),

                         'roc':roc,
                
                         'precision_recall_curve':prc
                         }
 
                experimental_result = ResultConfig(result)
                experimental_result.write_to_db()



