import pandas as pd
from HectopuntenFeatureFactory import HectopuntenFeatureFactory, hectopunten_driver
from utils.misc_utils import connect_rds

########################
### Configuration
########################

hectopunten_table = 'rws_clean.hectopunten'
hectopunten_mapping_table = 'segmentation.one_km'
write_to_schema_name = 'features_hectopunten_agg'
hectopunten_rollup_table = 'rws_clean.hectopunten_rollup'

conn = connect_rds()


ff = hectopunten_driver(hectopunten_table=hectopunten_table, hectopunten_mapping_table=hectopunten_mapping_table,
       conn=conn, write_to_schema_name=write_to_schema_name,
       hectopunten_rollup_table=hectopunten_rollup_table, write_frame=True)

conn.close()
