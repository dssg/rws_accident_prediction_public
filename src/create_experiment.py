import sys
from utils import write_exp_utils
import pandas as pd
from utils import misc_utils
import psycopg2
from psycopg2.extras import Json, DictCursor

def main(argv):
    print(argv[1])
    w = write_exp_utils.ExperimentConfig(argv[1], argv[2])
    print("writing {} to database".format(argv[1]) )
    w.write_to_db()# write experiment on database 

    # check if the experiment is  written correctly
    q = 'select experiment_id from rws_experiment.experiment_table order by experiment_id desc limit 1;'	
    conn = misc_utils.connect_rds()
    print(pd.read_sql(q, conn))  

if __name__== '__main__':
    main(sys.argv)
