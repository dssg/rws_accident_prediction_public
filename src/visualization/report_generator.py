from visualization.visualize import ParameterResult
import pandas as pd
import psycopg2
from utils import misc_utils
import sys

def main(argv):
    conn = misc_utils.connect_rds()
    experiment_id = argv[1]
    query = f'''select * from rws_experiment.result_table where experiment_id={experiment_id}'''
    df_all = pd.read_sql(query,conn)
    models = df_all.model.unique()
    print(models)#list of models in this experiement
    best_model_hyp = {"model":"","param":"","r8":0}
    best_r8_score = -1
    for model in models:
        q=f'''select * from rws_experiment.result_table where experiment_id={experiment_id} and model=\'{model}\'  '''
        df = pd.read_sql(q,conn)
        params=df.parameters_id.unique()
        for param in params:
            param_result = ParameterResult(experiment_id,model,param)  
            p_8,r_8=param_result.generate_report()#generate a report for each model+hyperparameters    
            if r_8>best_r8_score:
                best_r8_score=r_8
                best_model_hyp["model"]=model
                best_model_hyp["param"]=param    
                best_model_hyp["r8"]=r_8   
        print(best_model_hyp)#print out model+hyperparameters with highest f-1 score
    conn.close()    



if __name__ == "__main__":
   main(sys.argv)

