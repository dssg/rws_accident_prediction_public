import pandas as pd
import psycopg2

import numpy as np

from sklearn.linear_model import LogisticRegression
import sklearn.preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import ParameterGrid
from sklearn.metrics import roc_auc_score
from itertools import product
from random import choice
from datetime import datetime

import signal

import sys

from sklearn.metrics import *
from utils.write_exp_utils import ResultConfig
from utils.misc_utils import connect_rds


from utils.orchestra_utils import return_timed_spine, sampler, time_mark_generator, generate_temporal_features, preprocessor, rinse_spines, temporal_split
from models.train import train_loop
from utils.write_exp_utils import Experiment


def main(conn, argv):

    experiment_ids = sys.argv[1:]
    experiment_ids = [int(e) for e in experiment_ids]    
    
    for experiment_id in experiment_ids:
        print("Running experiment id:",experiment_id)
        experiment = Experiment(experiment_id, conn)
        spine_creator = """
        -- this would need to be called through a Python Script and replace variables
        -- such as rounding, lag, segmentation table to join and so-forth.
        -- this joins ongeval to hectopunten through the ongeval_hectopunten link
        -- date rounding decides the granularity at which we want to make prediction.
        -- I am not sure if I can do 

        select
        date_trunc('hour', datetime) + date_part('minute', datetime)::int / {0} * interval '{0} min' as datetime_rounded,
        seg.hectokey_merged,
        count(*) as accidents
        from rws_clean.ongevallen as o
        inner join {1} as seg
        on seg.hectokey = o.hectokey
        group by datetime_rounded, seg.hectokey_merged
        """.format(experiment.TIME_GRANULARITY, experiment.SEGMENTATION_TABLE)

        spine = pd.read_sql(spine_creator, con=conn)

        spine_label = return_timed_spine(spine, experiment.LABEL_START, experiment.LABEL_END)
        spine_test = return_timed_spine(spine, experiment.TEST_LABEL_START, experiment.TEST_LABEL_END )

        #duplicate the rows where there are > 1 accidents
        spine_label = spine_label.loc[spine_label.index.repeat(spine_label['accidents'])].reset_index(drop=True)
        spine_test = spine_test.loc[spine_test.index.repeat(spine_test['accidents'])].reset_index(drop=True)

        print(
            """
            ##################################
            ### CALLING TRAIN_LOOP
            ##################################
            """) 

        
        train_loop(spine_label=spine_label, spine_test=spine_test, experiment=experiment, conn=conn)
        #close connection
    
    conn.close()
    
def signal_handler(signal, frame):
    print("\nQuiting .. closing connections")
    conn.close()
    sys.exit(0)

if __name__== '__main__':
    conn = connect_rds()
    signal.signal(signal.SIGINT, signal_handler)
    main(conn, sys.argv)

