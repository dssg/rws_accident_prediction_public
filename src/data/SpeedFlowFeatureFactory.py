from string import Template
import pandas as pd
import psycopg2
import time
import re
pd.options.mode.chained_assignment = None

from utils.misc_utils import connect_rds


########################
### Configuration
########################

# name of schema that holds flow features
features_schema = "features_speedflow_agg"
features_segment_table_no_version = "fifteenmin_seg"
# location (schema + table) of speed and flow data
speedflow_loc = "rws_clean.speedflow_15min"

# name of tables with flow features schema for aggregating across hecto and across segments respectively
#NOTE: do not include version number for table below
features_hecto_table_no_version = "fifteenmin_hecto"
# name of schema that holds segmentation tables
segmentation_schema = "segmentation"
# location (schema + table) of table for loop network
networklink_loc = "rws_clean.flow_network"


# name of segmentation tables within segmentation schema that will be run
seg_name = ['fifteen_km','ten_km', 'five_km']

# name of files used to generate flow features
# make sure the file name includes the "per_hecto","across_seg", or "across_time" so it is clear what the task is
filenames = ["10a_generate_flow_features_per_hecto_15min.sql",
            "10b_generate_flow_features_across_seg_15min.sql",
            "10c_generate_flow_features_across_time_15min.sql"]



#Use set of files below if just generating features for a new segmentation scheme
#filenames = ["10b_generate_flow_features_across_seg_15min.sql",
            #"10c_generate_flow_features_across_time_15min.sql"]            



########################
### Generating Features
########################

#"features_schema": features_schema,
#add config parameters into a dictionary
template_dict = {"features_schema": features_schema,
                 "features_hecto_table_no_version": features_hecto_table_no_version,
                 "features_segment_table_no_version": features_segment_table_no_version,
                 "speedflow_loc": speedflow_loc,
                 "segmentation_schema": segmentation_schema,
                 "networklink_loc": networklink_loc
                }

# connect to database
conn = connect_rds()

#get current version number for the table that stores features values per hecto (referred to as hecto table)
q = '''SELECT * FROM information_schema.tables WHERE table_schema = '{0}';
        '''.format(template_dict['features_schema'])
        
existing_feature_tables = pd.read_sql(q, con=conn)

hecto_version = 1
segment_version = 1
if existing_feature_tables.shape[0]>0:
    existing_hecto_table = existing_feature_tables.loc[existing_feature_tables.table_name.\
                                                       str.contains(template_dict['features_hecto_table_no_version']\
                                                                    .split('.')[-1]),:]
    
    if existing_hecto_table.shape[0]>0:
        existing_hecto_table['version'] = existing_hecto_table.table_name.map(lambda x: int(x.split('_')[-1].replace('v','')))
        
        #if we are making a new hecto table, then update the hecto version.  Otherwise keep the same version
        if 'per_hecto' in filenames:
            hecto_version = existing_hecto_table.version.max() + 1
        else:
            hecto_version = existing_hecto_table.version.max()
    
            
template_dict['features_hecto_table'] = template_dict['features_hecto_table_no_version'] + "_v" + str(hecto_version)

# now for each segmentation schema, run the three files to generate flow features
with conn.cursor() as cursor:
    for s in seg_name:
        print(s)
        template_dict['segmentation_table'] = s
        search_string = template_dict['features_segment_table_no_version'] + "_" + s

        # get version number for that segmentation scheme, and update version number if making new table for features by segment
        existing_segment_table = existing_feature_tables.loc[existing_feature_tables.table_name\
                                                       .str.contains(search_string),:]
        if existing_segment_table.shape[0]>0:
            existing_segment_table['version'] = existing_segment_table.table_name.map\
            (lambda x: int(x.split('_')[-1].replace('v','')))
            if 'across_seg' in filenames:
                segment_version = existing_segment_table.version.max() + 1
            else:
                segment_version = existing_segment_table.version.max()

        template_dict['features_segment_table'] = search_string + "_v" + str(segment_version)
        
        # run through the files
        for f in filenames:
            start = time.time()
            sql = Template(open("../../database/" + f, "r").read())
            cursor.execute(sql.substitute(template_dict))
            conn.commit()
            end = time.time()
            print("%s took %d seconds" % (f, (end-start)))

# close the connection        
conn.close()
        

