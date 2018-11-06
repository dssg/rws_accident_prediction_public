from utils import misc_utils
from datetime import datetime
from psycopg2.extras import Json, DictCursor
import psycopg2
import importlib.util
import pandas as pd
import yaml
from functools import reduce

num2words = {1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', \
             6: 'six', 7: 'seven', 8: 'eight', 9: 'nine', 10: 'ten', \
            11: 'eleven', 12: 'twelve', 13: 'thirteen', 14: 'fourteen', \
            15: 'fifteen', 16: 'sixteen', 17: 'seventeen', 18: 'eighteen', \
            19: 'nineteen', 20: 'twenty', 30: 'thirty', 40: 'forty', \
            50: 'fifty', 60: 'sixty', 70: 'seventy', 80: 'eighty', \
            90: 'ninety', 0: 'zero'} 

def create_parameter_id(parameter_dict):
    '''
    Sorts the keys and adds underscore between key and value.
    Changes nothing else.
    Used to create the parameter code before pushing results to result_table
    '''

    codes = []
    for key in sorted(parameter_dict):
        codes.append(key)
        codes.append('_')
        codes.append(str(parameter_dict[key]))
        codes.append('_')
    
    return ''.join(codes).strip('_')

def update_table(sql):
    conn = None
    try:
        conn = misc_utils.connect_rds()
        cur = conn.cursor(cursor_factory=DictCursor)        
        cur.execute(sql)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    
    
def update_results_eval(variable, value, result_id):
    
    sql = f"""update rws_experiment.result_table
                  set {variable} = {value}
                  where result_id = {result_id};"""
    update_table(sql)
    
def update_raw_eval(value, result_id):
    
    sql = f"""update rws_experiment.raw_y_score
              set precision_recall_curve = {value}
              where result_id = {result_id};"""
    update_table(sql)
    
class ExperimentConfig(object):
    def __init__(self,config_filepath:str, feature_config_filepath:str):
        '''
        reads a configuration file and creates class variables for each high level param
        '''
#         spec = importlib.util.spec_from_file_location("module.name", config_filepath)
#         config = importlib.util.module_from_spec(spec)
#         spec.loader.exec_module(config)
       
        with open(config_filepath) as f:
            config = yaml.load(f.read())
        
        with open(feature_config_filepath) as f:
            feature_config = yaml.load(f.read())
            
        
        self.EXPERIMENT_DATETIME = str(datetime.now()) 
        #READING
        self.TIME_GRANULARITY = config['panel_config']['time_granularity']
        self.SEGMENTATION_TABLE = config['panel_config']['segmentation_table']
        self.TIME_LAG_MINUTES = config['panel_config']['time_lag_minutes'] 
        self.WEATHER_STATION_MAPPING_TABLE = config['panel_config']['weather_station_mapping_table']

        ###########################
        #### SAMPLING PARAMS
        ###########################

        self.SAMPLE_TRAIN = config['panel_config']['sample_train']
        self.SAMPLE_VAL = config['panel_config']['sample_val']
        self.SAMPLE_TEST = config['panel_config']['sample_test']
        self.TRAIN_SAMPLE_METHOD_PARAMS = Json(config['panel_config']['train_sample_method_params'])
        self.VAL_SAMPLE_METHOD_PARAMS = Json(config['panel_config']['val_sample_method_params'])
        self.TEST_SAMPLE_METHOD_PARAMS = Json(config['panel_config']['test_sample_method_params'])


        #############################
        #### FEATURES CONFIGURATIONS
        #############################
        self.FEATURES_KNMI_CONFIG = feature_config['features_knmi_config']
        self.FEATURES_HECTOPUNTEN_CONFIG =  feature_config['features_hectopunten_config']
        self.FEATURES_FLOW_CONFIG = feature_config['features_flow_config']
        self.FEATURES_TEMPORAL_CONFIG= feature_config['features_temporal_config']
        self.FEATURES_ONGEVALLEN_CONFIG = feature_config['features_ongevallen_config']
        
#       #######FEATURE LISTS ######(
        ## Putting all features into FEATURE_X_LIST variables.
    
        if self.FEATURES_KNMI_CONFIG['use_all_features'] == True:
            self.FEATURES_KNMI_LIST = self._get_all_features(self.FEATURES_KNMI_CONFIG)
            self.FEATURES_KNMI_CONFIG['features_list'] = self.FEATURES_KNMI_LIST
            
        else:
            self.FEATURES_KNMI_LIST = [f.strip() for f in self.FEATURES_KNMI_CONFIG['features_list']]
            self.FEATURES_KNMI_LIST = list(set(self.FEATURES_KNMI_LIST))
            
        
        if self.FEATURES_HECTOPUNTEN_CONFIG['use_all_features'] == True:
            self.FEATURES_HECTOPUNTEN_LIST = self._get_all_features(self.FEATURES_HECTOPUNTEN_CONFIG)
            self.FEATURES_HECTOPUNTEN_CONFIG['features_list'] = self.FEATURES_HECTOPUNTEN_LIST
        else:
            self.FEATURES_HECTOPUNTEN_LIST = [f.strip() for f in self.FEATURES_HECTOPUNTEN_CONFIG['features_list']]
            self.FEATURES_HECTOPUNTEN_LIST = list(set(self.FEATURES_HECTOPUNTEN_LIST))
        
        ###
            
        if self.FEATURES_FLOW_CONFIG['use_all_features'] == True:
            self.FEATURES_FLOW_LIST = self._get_all_features(self.FEATURES_FLOW_CONFIG)
            self.FEATURES_FLOW_CONFIG['features_list'] = self.FEATURES_FLOW_LIST
            
        else:
            self.FEATURES_FLOW_LIST = [f.strip() for f in self.FEATURES_FLOW_CONFIG['features_list']]
            self.FEATURES_FLOW_LIST = list(set(self.FEATURES_FLOW_LIST))

        ###
            
        if self.FEATURES_TEMPORAL_CONFIG['use_all_features'] == True:
            self.FEATURES_TEMPORAL_LIST = self._get_all_features(self.FEATURES_TEMPORAL_CONFIG)
            self.FEATURES_TEMPORAL_CONFIG['features_list'] = self.FEATURES_TEMPORAL_LIST
        else:
            self.FEATURES_TEMPORAL_LIST = [f.strip() for f in self.FEATURES_TEMPORAL_CONFIG['features_list']]
            self.FEATURES_TEMPORAL_LIST = list(set(self.FEATURES_TEMPORAL_LIST))

        ###
        
        if self.FEATURES_ONGEVALLEN_CONFIG['use_all_features'] == True:
            self.FEATURES_ONGEVALLEN_LIST = self._get_all_features(self.FEATURES_ONGEVALLEN_CONFIG)
            self.FEATURES_ONGEVALLEN_CONFIG['features_list'] = self.FEATURES_ONGEVALLEN_LIST
        else:
            self.FEATURES_ONGEVALLEN_LIST = [f.strip() for f in self.FEATURES_ONGEVALLEN_CONFIG['features_list']]
            self.FEATURES_ONGEVALLEN_LIST = list(set(self.FEATURES_ONGEVALLEN_LIST))

       

        self.VALIDATION_CONFIG = Json(config['panel_config']['validation_config'])
        
        self.FEATURES_TOTAL = reduce(list.__add__,
                                [self.FEATURES_KNMI_LIST,
                                 self.FEATURES_HECTOPUNTEN_LIST,
                                 self.FEATURES_FLOW_LIST,
                                 self.FEATURES_TEMPORAL_LIST,
                                 self.FEATURES_ONGEVALLEN_LIST],
                               )
        
        self.CATEGORICAL_FEATURES_LIST = [feature for feature in self.FEATURES_TOTAL if feature.split('_')[-1] == 'cat'] 
        self.NUMERICAL_FEATURES_LIST = [feature for feature in self.FEATURES_TOTAL if feature.split('_')[-1] == 'num']
       
        
    
        ############################
        ## MODEL PARAMS
        ###########################
        self.CLASSIFIERS = Json(config['classifier_config'])
        self.GRID_PARAMETERS = Json(config['grid_config'])
        self.RANDOM_SEED = config['panel_config']['random_seed']
        self.EXP_REMARKS = config['panel_config']['exp_remarks']
        ## by default the configuration is invalid - then validate invalidates if something is wrong
        self.valid = True
        
    def _get_all_features(self, feature_config):
        table_name = feature_config['table_name']
        meta_columns = feature_config['meta_columns']
        
        q = '''SELECT * FROM information_schema.columns WHERE table_schema = '{0}' AND table_name='{1}';'''.format(table_name.split('.')[0], table_name.split('.')[1]) 
        conn = misc_utils.connect_rds()
        tbl_info = pd.read_sql(q, conn)
        conn.close()
#         print(tbl_info.columns)
        return tbl_info.loc[~tbl_info.column_name.isin(meta_columns),"column_name"].tolist()
    
    def validate(self):
        '''
        validation logic is here, if all tests pass then it sets self.valid = True
        '''
        #Connection to PostGreSQL
        conn = None
        try:
            conn = misc_utils.connect_rds()
            cur = conn.cursor(cursor_factory=DictCursor)
        
            #TODO! Check if use_all_feautures if is False then custom_features_list is provided.
            
            ## Check if there are features without num/cat marking
            unmarked_features = list(set(self.FEATURES_TOTAL) - set(self.CATEGORICAL_FEATURES_LIST) - set(self.NUMERICAL_FEATURES_LIST))
            if len(unmarked_features)>0:
                self.valid = False
                print("ERROR: These features don't have cat/num \n",','.join(unmarked_features))
            
            ## Validating if TIME_GRANULARITY IS IN MINUTES AND INTEGER
            tg = self.TIME_GRANULARITY        
            if type(tg)!=int or tg>60 or tg<=0:
                print('\n ERROR: TIME GRANULARITY HAS TO BE AN INT BETWEEN 1 AND 60 \n')
                self.valid = False
            
            print('Validating if columns exists for features mentioned in config')
            for table, feature_list in zip(
                [self.FEATURES_KNMI_CONFIG['table_name'],
                 self.FEATURES_FLOW_CONFIG['table_name'],
                 self.FEATURES_HECTOPUNTEN_CONFIG['table_name'],
                 self.FEATURES_TEMPORAL_CONFIG['table_name'],
                 self.FEATURES_ONGEVALLEN_CONFIG['table_name']
                ],
                [
                 self.FEATURES_KNMI_LIST,
                 self.FEATURES_FLOW_LIST,
                 self.FEATURES_HECTOPUNTEN_LIST,
                 self.FEATURES_TEMPORAL_LIST,
                 self.FEATURES_ONGEVALLEN_LIST,
                ]):
                print('Validating', table, 'to check if columns mentioned exist')
                q = '''SELECT * FROM information_schema.columns WHERE table_schema = '{0}' AND table_name='{1}'; '''.format(table.split('.')[0], table.split('.')[1])
                tbl_info = pd.read_sql(q, conn)
                missing_columns = list(set(feature_list)-set(tbl_info.column_name))
                if len(missing_columns)>0:
                    print('\n!!ERROR!!\n',','.join(missing_columns), '\n', 'are missing in: ', table, '\n')
                    self.valid = False 


            #list of tables
            print("VALIDATING: If these tables exist")
            tables = [self.SEGMENTATION_TABLE,
                      self.WEATHER_STATION_MAPPING_TABLE,
                      ]
                      #add more table when they are ready e.g. self.FEATURES_TEMPORAL_TABLE


            # Checking if categorical numerical features mentioned equal features described otherwise.
            
            total_features_cat_num = self.CATEGORICAL_FEATURES_LIST[:] + self.NUMERICAL_FEATURES_LIST[:]
            # print(total_features_cat_num)
            
            
            difference = list(set(self.FEATURES_TOTAL) - set(total_features_cat_num))
            if len(difference)>0:
                print('These columns are in total_features but not defined in cat-num lists:', ','.join(difference))
                self.valid = False

            
            difference = list( set(total_features_cat_num)- set(self.FEATURES_TOTAL))
            if len(difference)>0:
                print('These columns are defined in cat-num lists but not in total features:', ','.join(difference))
                self.valid = False
     

            #Connection to PostGreSQL
            ####VALIDATE TABLES
            validate_table = []
            for t in tables:
                schema, name = t.split(".")
                print('Validating if ',schema,name, 'exists')
                sql = f"""SELECT EXISTS (
                            SELECT 1
                               FROM   information_schema.tables
                               WHERE  table_schema = '{schema}'
                               AND    table_name = '{name}'
                         );"""
                cur.execute(sql)
                conn.commit()
                row = cur.fetchone()
                validate_table.append(row[0])
 
            if len(validate_table)>=1:
                if False in validate_table:
                    self.valid=False
                else:
                    print("All these tables exist!")
            cur.close()

            #Check if feature tables mentioned are consitent with the granularities mentioned.
            #note: this only works with space granularity since naming convention for time granularity is inconsistent. 
            #note: KNMI table name does not contain any info about granularity
            #note: temporal table has no space granularity information
            for config in [self.FEATURES_FLOW_CONFIG,self.FEATURES_HECTOPUNTEN_CONFIG, self.FEATURES_ONGEVALLEN_CONFIG]:     
                if num2words[config['space_granularity_km']]+'_km' in config['table_name']:
                    print(config['table_name'],'-> this table\'s name is consistent with the granularity mentioned.' ) 
                else:
                    print(config['table_name'],'-> the table name is not consistent with the granularity mentioned, please check again!')
                    self.valid=False

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        #Close connection
       


    def __create_info_tuple(self):
        """Experiment info 
            return a tuple for write to db function"""
        info = (self.EXPERIMENT_DATETIME,
                self.TIME_GRANULARITY,
                self.SEGMENTATION_TABLE,
                self.TIME_LAG_MINUTES,
                self.WEATHER_STATION_MAPPING_TABLE,
                
                self.SAMPLE_TRAIN,
                self.SAMPLE_VAL,
                self.SAMPLE_TEST,
                self.TRAIN_SAMPLE_METHOD_PARAMS,
                self.VAL_SAMPLE_METHOD_PARAMS,
                
                self.TEST_SAMPLE_METHOD_PARAMS,
                Json(self.FEATURES_KNMI_CONFIG),
                Json(self.FEATURES_HECTOPUNTEN_CONFIG),
                Json(self.FEATURES_FLOW_CONFIG),
                Json(self.FEATURES_TEMPORAL_CONFIG),
                
                Json(self.FEATURES_ONGEVALLEN_CONFIG),
                self.VALIDATION_CONFIG,
                self.CLASSIFIERS,
                self.GRID_PARAMETERS,
                self.RANDOM_SEED,
                
                self.EXP_REMARKS)  
        return info 
     
    def write_to_db(self):
        self.validate()
        if self.valid == True:
            ## writing logic
            print("Answer name of experiment - don't be lazy and describe well:")
            self.EXP_REMARKS = input()
            experiment_info = self.__create_info_tuple()
            sql = """INSERT INTO 
                         rws_experiment.experiment_table(
                         
                         experiment_datetime,
                         time_granularity,
                         segmentation_table,
                         time_lag_minutes,                       
                         weather_station_mapping_table,
                         
                         sample_train,
                         sample_val,
                         sample_test,
                         train_sample_method_params,                         
                         val_sample_method_params,
                         
                         test_sample_method_params,
                         features_knmi_config,
                         features_hectopunten_config,
                         features_flow_config,
                         features_temporal_config,
                         
                         features_ongevallen_config,
                         validation_config,
                         classifiers,
                         grid_parameters,
                         random_seed,
                         
                         exp_remarks) 
                    VALUES(%s,%s,%s,%s,%s,  %s,%s,%s,%s,%s,  %s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s, %s);"""

            #Connection to PostGreSQL
            conn = None
            try:
                conn = misc_utils.connect_rds()
                cur = conn.cursor(cursor_factory=DictCursor)
                cur.execute(sql, experiment_info)
                conn.commit()
                #close connection
                cur.close()
                
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn is not None:
                    conn.close()
            #Close connection
                
        else:
         #Raise Error saying configuration is not valid 
            print("this configuration is not VALID!")

class Experiment(object):
    def __init__(self, experiment_id, conn):
        '''
        reads from the database
        loads up highlevel variables as high level parameters of this experiment
        does processing ie converting into correct datatypes from string
        generates the parameters I need
        '''
        
        q = "select * from rws_experiment.experiment_table as et where et.experiment_id={0};".format(
            experiment_id)
        
        experiment_config = pd.read_sql(q, con=conn).iloc[0]
        
        self.EXPERIMENT_ID = experiment_id
        
        #update the sql query to create int column for TG #todo!
        self.TIME_GRANULARITY = int(experiment_config['time_granularity']) 
        
        self._granularity_mappings = {
            60:'60min',
            30:'30min',
            15:'15min' #todo! check if these works as inputs for Python.
        }
        self.TIME_GRANULARITY_PANDAS = self._granularity_mappings[self.TIME_GRANULARITY]
        
        self.SEGMENTATION_TABLE = experiment_config['segmentation_table']
        self.TIME_LAG_MINUTES = experiment_config['time_lag_minutes']
        self.WEATHER_STATION_MAPPING_TABLE = self.SEGMENTATION_TABLE + '_ws' #relies on naming convention to hold well. @Anne
        
        self.SAMPLE_TRAIN = experiment_config.sample_train
        self.SAMPLE_VAL = experiment_config.sample_val
        self.SAMPLE_TEST = experiment_config.sample_test
        
        # this will break now as can is chaning this #todo!
        self.VALIDATION_CONFIG = experiment_config['validation_config']
        self.LABEL_START = pd.to_datetime(experiment_config['validation_config']['label_start'])
        self.LABEL_END = pd.to_datetime(experiment_config['validation_config']['label_end'])
        self.TEST_LABEL_START = pd.to_datetime(experiment_config['validation_config']['test_start'])
        self.TEST_LABEL_END = pd.to_datetime(experiment_config['validation_config']['label_end'])

        self.TRAIN_SAMPLE_METHOD_PARAMS = experiment_config.train_sample_method_params
        self.TRAIN_SAMPLE_METHOD_PARAMS['merged_hectopunten_name'] = self.SEGMENTATION_TABLE
        self.TRAIN_SAMPLE_METHOD_PARAMS['time_granularity'] = self.TIME_GRANULARITY_PANDAS
        
        self.VAL_SAMPLE_METHOD_PARAMS = experiment_config.val_sample_method_params
        self.VAL_SAMPLE_METHOD_PARAMS['merged_hectopunten_name'] = self.SEGMENTATION_TABLE
        self.VAL_SAMPLE_METHOD_PARAMS['time_granularity'] = self.TIME_GRANULARITY_PANDAS
        
        self.TEST_SAMPLE_METHOD_PARAMS = experiment_config.test_sample_method_params
        self.TEST_SAMPLE_METHOD_PARAMS['merged_hectopunten_name'] = self.SEGMENTATION_TABLE
        self.TEST_SAMPLE_METHOD_PARAMS['time_granularity'] = self.TIME_GRANULARITY_PANDAS
        self.TEST_SAMPLE_METHOD_PARAMS['positive_perc'] = 0.03 #not need but is required as param by the method
        #########################
        #### FEATURES - TABLES, LIST & NUMERIC/CATEGORICAL
        #########################
#         print(experiment_config.features_knmi_config.keys())
        
    
        self.FEATURES_KNMI_CONFIG = experiment_config.features_knmi_config
        self.FEATURES_HECTOPUNTEN_CONFIG = experiment_config.features_hectopunten_config
        self.FEATURES_FLOW_CONFIG = experiment_config.features_flow_config
        self.FEATURES_TEMPORAL_CONFIG = experiment_config.features_temporal_config
        self.FEATURES_ONGEVALLEN_CONFIG = experiment_config.features_ongevallen_config
        
    
        self.FEATURES_KNMI_TABLE = experiment_config.features_knmi_config["table_name"]
        self.FEATURES_HECTOPUNTEN_TABLE = experiment_config.features_hectopunten_config['table_name']
        self.FEATURES_FLOW_TABLE = experiment_config.features_flow_config['table_name']
        self.FEATURES_TEMPORAL_TABLE = experiment_config.features_temporal_config['table_name']
        self.FEATURES_ONGEVALLEN_TABLE = experiment_config.features_ongevallen_config['table_name']
        
        self.FEATURES_KNMI_LIST = experiment_config.features_knmi_config['features_list']
        self.FEATURES_HECTOPUNTEN_LIST = experiment_config.features_hectopunten_config['features_list']
        self.FEATURES_TEMPORAL_LIST = experiment_config.features_temporal_config['features_list']
        self.FEATURES_FLOW_LIST = experiment_config.features_flow_config['features_list']
        self.FEATURES_ONGEVALLEN_LIST = experiment_config.features_ongevallen_config['features_list']
        
        
        self.FEATURES_TOTAL = reduce(list.__add__,
                                [self.FEATURES_KNMI_LIST,
                                 self.FEATURES_HECTOPUNTEN_LIST,
                                 self.FEATURES_FLOW_LIST,
                                 self.FEATURES_TEMPORAL_LIST,
                                 self.FEATURES_ONGEVALLEN_LIST,
                                ])
        
        self.CATEGORICAL_FEATURES_LIST = [feature for feature in self.FEATURES_TOTAL if feature.split('_')[-1] == 'cat'] 
        self.NUMERICAL_FEATURES_LIST = [feature for feature in self.FEATURES_TOTAL if feature.split('_')[-1] == 'num']
       
        
        ############################
        ## MODEL PARAMS
        ###########################
        self.RANDOM_SEED = experiment_config.random_seed
        self.CLASSIFIERS = experiment_config.classifiers
        self.GRID_PARAMETERS = experiment_config.grid_parameters
        
        
        
        ######################################################
        #### GENERATED PARAMS - THESE ARE NOT DEFINED IN CONFIG
        ######################################################
        
        
class ResultConfig(object):

    def __init__(self,result:dict):

        '''Insert experiment details and  raw scores to the databse

        '''

        self.EXPERIMENT_ID = result['experiment_id']

        self.VAL_SET_ID = result['val_set_id']

        self.RESULT_DATETIME = str(datetime.now())

        self.MODEL = result['model']

        self.PARAMETERS_ID = result['parameters_id']

        self.PARAMETERS =  Json(result['parameters'])
        
        self.SPLIT_INFO = Json(result['split_info'])
        
        self.TRAIN_SIZE = result['train_size']
        
        self.VAL_SIZE = result['val_size']

        self.ACTUAL_FEATURES_SIZE= result['actual_features_size']
        
        self.ACCIDENT_PCT_TRAIN_PRE_SAMPLE = result['accident_pct_train_pre_sample']
        
        self.ACCIDENT_PCT_VAL = result['accident_pct_val']
        
        self.P_8_DAYSHIFT = result['p_8_dayshift']
        
        self.P_1_NIGHTSHIFT = result['p_1_nightshift']
        
        self.R_8_DAYSHIFT = result['r_8_dayshift']
        
        self.R_1_NIGHTSHIFT = result['r_1_nightshift']        
        
        self.AUC_ROC = result['auc_roc']

        self.PICKLE_PATH = result['pickle_path']

        self.REMARKS = result['remarks']


        #store these raw scores in a separate table
        self.Y_TRUE = result['y_true']

        self.Y_SCORES = result['y_scores']
        
        self.SPACE_TIME = Json(result['space_time'])

        self.ROC = Json(result['roc'])

        self.PRECISION_RECALL_CURVE = Json(result['precision_recall_curve'])

    def validate(self):

        '''

        validation logic is here, if all tests pass then it sets self.valid = True

        '''

        #NotImplementedinV1

        self.valid = True

    def __create_info_tuple(self):

        """Experiment info

            return a tuple for write to db function"""

        info = ( self.EXPERIMENT_ID,

            self.VAL_SET_ID,

            self.RESULT_DATETIME,

            self.MODEL,

            self.PARAMETERS_ID,

            self.PARAMETERS,
            
            self.SPLIT_INFO,
            
            self.TRAIN_SIZE,
                
            self.VAL_SIZE,

            self.ACTUAL_FEATURES_SIZE,
        
            self.ACCIDENT_PCT_TRAIN_PRE_SAMPLE,
            
            self.ACCIDENT_PCT_VAL,
            
            self.P_8_DAYSHIFT,
            
            self.P_1_NIGHTSHIFT,
            
            self.R_8_DAYSHIFT,
            
            self.R_1_NIGHTSHIFT,        

            self.AUC_ROC,

            self.PICKLE_PATH,

            self.REMARKS,)
        return info



    def write_to_db(self):

        self.validate()

        if self.valid == True:

               ## writing logic

            result_info = self.__create_info_tuple()
            
            
            sql_result = """INSERT INTO

                         rws_experiment.result_table(

                             experiment_id,

                             val_set_id,

                             result_datetime,

                             model,

                             parameters_id,

                             parameters,
                             
                             split_info,
                             
                             train_size,
                             
                             val_size,

                             actual_features_size,
                             
                             accident_pct_train_pre_sample,
                             
                             accident_pct_val,
                             
                             p_8_dayshift,
                             
                             p_1_nightshift,
                             
                             r_8_dayshift,
                             
                             r_1_nightshift,

                             auc_roc,

                             pickle_path,

                             remarks )

                     VALUES(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s)
                     RETURNING result_id;"""
            
            sql_raw_score = """INSERT INTO

                         rws_experiment.raw_y_score(
                             result_id,

                             experiment_id,

                             val_set_id,
   
                             y_true,

                             y_scores,

                            space_time,

                            roc,
 
                            precision_recall_curve)

                     VALUES(%s,%s,%s,%s,%s, %s,%s,%s);"""
            #Connection to PostGreSQL
            conn = None
            try:
                conn = misc_utils.connect_rds()
                cur = conn.cursor(cursor_factory=DictCursor)
                cur.execute(sql_result, result_info)#write to result_table
                result_id = cur.fetchone()[0]
                result_y_raw = (result_id,self.EXPERIMENT_ID,self.VAL_SET_ID,self.Y_TRUE,self.Y_SCORES,self.SPACE_TIME,self.ROC,self.PRECISION_RECALL_CURVE,)
                cur.execute(sql_raw_score, result_y_raw)#write to raw_y_score table
                conn.commit()
                #close connection
                cur.close()
                
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn is not None:
                    conn.close()
            #Close connection
 
        else:

         #Raise Error saying configuration is not valid

            print("this configuration is not VALID!")



