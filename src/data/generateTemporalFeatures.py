import pandas as pd

from TemporalFeatureFactory import TemporalFeatureFactory, temporal_driver
from utils.misc_utils import connect_rds


### 
# DEFINE SETTINGS TO CREATE TEMPORAL FEATURES
###

time_granularity = '60min'
start_date = '2016-01-01'
end_date = '2018-02-01'
conn = connect_rds()
schema_name = 'features_temporal_agg'

### 
# CREATE TEMPORAL TABLE
###

t = temporal_driver('60min', '2016-01-01', '2018-02-01', conn, 'features_temporal_agg')
