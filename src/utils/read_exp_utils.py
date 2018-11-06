from utils.misc_utils import connect_rds
import pandas as pd
from psycopg2.extras import Json, DictCursor
import psycopg2

def read_sql(sql):
    conn = connect_rds()
    try:
        df=pd.read_sql(sql,conn)
        return df

    except (exception, psycopg2.databaseerror) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()   

def read_experiment(experiment_id: int):
    conn = connect_rds()
    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        sql = f"""select
                      classifiers, grid_parameters, random_seed 
                 from 
                    rws_experiment.experiment_table
                 where
                    experiment_id={experiment_id};"""
        cur.execute(sql)
        out = cur.fetchone()
        return out
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()   

def read_experiment_result(experiment_id:int, model:str, parameters_id:str):
    conn = connect_rds()
    try:
        sql_result = f"""select
                     *
                 from 
                    rws_experiment.result_table
                 where
                    experiment_id={experiment_id} 
                    and model=\'{model}\'
                    and parameters_id=\'{parameters_id}\';"""

        result_df=pd.read_sql(sql_result,conn)

        result_ids = tuple(result_df.result_id.tolist())
        sql_raw = f"""select
                     *
                 from 
                    rws_experiment.raw_y_score
                 where
                    result_id in {result_ids};"""
        score_df=pd.read_sql(sql_raw,conn)
        result_score_df=pd.merge(result_df, score_df, on=['result_id','experiment_id','val_set_id'], how='inner')
        return result_score_df

    except (exception, psycopg2.databaseerror) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()   

def read_experiment_id_raw_result(experiment_id:int):
    conn = connect_rds()
    try:
        sql_result = f"""select
                     *
                 from 
                    rws_experiment.result_table
                 where
                    experiment_id={experiment_id};"""

        result_df=pd.read_sql(sql_result,conn)

        result_ids = tuple(result_df.result_id.tolist())
        sql_raw = f"""select
                     *
                 from 
                    rws_experiment.raw_y_score
                 where
                    result_id in {result_ids};"""
        score_df=pd.read_sql(sql_raw,conn)
        result_score_df=pd.merge(result_df, score_df, on=['result_id','experiment_id','val_set_id'], how='inner')
        return result_score_df

    except (exception, psycopg2.databaseerror) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()   

def read_raw_y(result_id:int):
    df = read_raw_y_db(result_id)
    
    #get the columns needed
    y_scores = df['y_scores'][0]
    y_true = df["y_true"][0]
    space_time = df["space_time"][0]
    
    #put in df
    df = pd.DataFrame({'space': space_time['space'], 'time': space_time['time'], 'y_scores': y_scores, 'y_true':y_true})
    df = df.sort_values(by=['time', 'y_scores'], ascending=False)
    
    return df

def read_results_for_experiment(experiment_id:int):
    conn = connect_rds()
    try:
        sql = f"""select
                   result_id
                from
                   rws_experiment.result_table
                where
                   experiment_id={experiment_id};"""
        df = pd.read_sql(sql, con=conn)
        return df['result_id'].values

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def read_empty_results():
    conn = connect_rds()
    try:
        sql = f"""select
                   result_id
                from
                   rws_experiment.result_table
                where
                   p_8_dayshift is null;"""
        df = pd.read_sql(sql, con=conn)
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def read_raw_y_db(result_id:int):
    conn = connect_rds()
    try:
        sql = f"""select
                   *
                from
                   rws_experiment.raw_y_score
                where
                   result_id={result_id};"""
        df = pd.read_sql(sql, con=conn)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()  
         
        
def read_experiment_result_db(experiment_id:int, model:str, parameters_id:str):
    conn = connect_rds()
    try:
        sql = f"""select
                   *
                from
                   rws_experiment.result_table
                where
                   experiment_id={experiment_id}
                   and model=\'{model}\'
                   and parameters_id=\'{parameters_id}\';"""
        df = pd.read_sql(sql, con=conn)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close() 
