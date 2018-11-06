import os
import json
import psycopg2
import pandas as pd
from datetime import datetime
###GIS###
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point
from shapely.ops import nearest_points

#########
def connect_rds():
    """Create a new database session based on configuration in PATH_CONFIG
    and return a new connection object."""
    
    PATH_CONFIG = os.getenv("RWS_ACCIDENT_PREDICTION_DIR") + "/db_config.json"
    
    with open(PATH_CONFIG) as f:
        config = json.load(f)

    user = config['user']
    password = config['password']
    db = config['db']
    host = config['host']
    port = config['port']

    conn_str = "host={} dbname={} user={} password={}".format(host, db, user, password)
    conn = psycopg2.connect(conn_str)

    return conn



def pandas_to_db(df, schema_name, table_name, write_path, conn, drop_table=True, ignore_not_null=True):
    '''
    For given dataframe, 
    
    1. write as csv to Pipeline/temp_files
    2. create table in RDS
    3. /copy to table
    
    Ideally I wouldn't want to write to templ_files and use buffer,
    but I have to because of csvsql usage.
    
    Make sure write path is relative.
    '''
    
    import os

    write_path = os.path.join(os.getcwd(), "temp_files", "t2.csv")
    #Need to represent null as \N for the eventual copy_from command.
    df.to_csv(write_path, index=False, na_rep='\\N' )
    
    # Run the csvsql command on all rows or atleast 5000 whichever is min.
    table_len = df.shape[0]
    table_sample = min(table_len, 5000)
    
    table_name = schema_name + '.' + table_name
    
    bashCommand = 'head -n{0} {1} | csvsql -i postgresql '.format(table_sample,
                              write_path)
    create_commands = os.popen(bashCommand).readlines()
    # replace stdin with table_name
    create_commands[0] = create_commands[0].replace('stdin',table_name)

    
    # replace create table with create table if not exists.
    create_commands[0] = create_commands[0].replace('CREATE TABLE','CREATE TABLE IF NOT EXISTS')
    
    if ignore_not_null == True:
        create_commands = [command.replace("NOT NULL","") for command in create_commands]
    
    # if you want the existing table to be dropped.
    if drop_table == True:
        create_commands.insert(0, 'DROP TABLE IF EXISTs {0}; \n'.format(table_name))
    

    q_create = ''.join(create_commands)

    # Print final query for validation
    print(q_create)
    
    cur = conn.cursor()
    cur.execute(q_create)
    conn.commit()

    # This opens the file again and puts it into database.
    with open(write_path) as f:
        next(f) #skipping first line
        cur.copy_from(f, table_name, sep=',')

    conn.commit()




def init_knmi_gis():
    """Create a KNMI geopandas dataframe for KNMI locations"""
    sql_stn = 'select * from rws_raw.knmi_station\
			   where "STN"!=265;'
    try:
        conn = connect_rds()
        df_stn = pd.read_sql(sql_stn, con=conn)
        geometry = [Point(xy) for xy in zip( df_stn["LAT(north)"],df_stn["LON(east)"],)]
        crs = {'init': 'epsg:4326'} #Coordinate reference system
        gdf = GeoDataFrame(df_stn, crs=crs, geometry=geometry)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return gdf

def find_nearest_station(input_lat:float, input_lon:float, gdf):
    """Return the ID of the nearest station
     
    Keyword arguments:
    input_lat -- latitude of the input
    input_lon -- longtitude of the input
    gdf -- geopandas dataframe of KNMAI stations
    """
    input_point = Point(input_lon,input_lat)
    nearest = gdf.geometry == nearest_points(input_point,gdf.unary_union)[1]
    station = gdf[nearest]["STN"].get_values()[0]
    return station

def get_weather(timestamp, station, weather_type, knmi_data):
    '''
    timestamp datetime
    timestamp of the accident - will be used to find the nearest weather timestamp
    
    station, float
    the weather station code for which the weather is required. This would be the closed weather
    station to the accident point
    
    weather_type string
    Eg 'DD' - the value of precipitation that's required
    These have to be codes as in the dataframe.
    
    knmi_data DataFrame
    KNMI dataframe
    
    return one value - whichever type the column requested is
    
    #TODO!: We are getting weather from the last hour, so even if it's 11:59 its get weather reported
    at 11 and not 12.
    
    '''
    
    
    
    if knmi_data.YYYYMMDD.dtype!='<M8[ns]':
        raise ValueError("Please convert YYYYMMDD column into datetime using pd.to_datetime first")
    
    date = pd.to_datetime(timestamp.date())
    hour = timestamp.hour
    
    
    t = knmi_data.loc[((knmi_data.YYYYMMDD==date)&(knmi_data.HH==hour)&(knmi_data.STN==station)), weather_type].values[0]
    
    return t
    
    
def plot_coords(lat,long, map_type='heatmap', tiles='Stamen Terrain', zoom_start=7.75, width=1000, height=1000):
    '''
    Easy function to quick-plot list of lat-longs while working.
    Imports folium etc and has some defaults (map zoomed into PT).
    
    lat: pandas series or list of lats
    long: pandas series or list of longs
    map_type str {'heatmap'|'markers'}        
    '''
    
    import folium
    import datetime as dt
    import random as rnd
    from folium.plugins import HeatMap
    coords = list(zip(lat, long))
    PT_COORD = [52.0297195, 4.5003863]
    
    map_pt = folium.Map(location=PT_COORD, zoom_start=7.75,width=1000, height=1000, tiles='Stamen Terrain')
    
    if map_type=='heatmap':
        HeatMap(coords).add_to(map_pt)
    
    if map_type=='markers':
        for location in coords:
            folium.CircleMarker(location=location, radius=2,color='orange', fill_color='orange').add_to(map_pt)
    
    return map_pt

####Calculating accident pct
def count_segments(seg_table_name:str):
    """Return number of unique segment"""
    
    sql=f"""select count(distinct(hectokey_merged) )
    from {seg_table_name};"""
    conn = connect_rds()
    out = pd.read_sql(sql, con=conn)
    conn.close()
    return out.values[0][0]

def calculate_num_population(seg_count,time_resolution,label_start,label_end):
    '''Return number of population based on space and time resolution'''
    time_diff=label_end-label_start
    days = time_diff.days
    minutes = days * 24 * 60
 
    return minutes*seg_count/time_resolution

def calculate_accident_pct(seg_table_name:str,time_resolution:int,label_start,label_end,pre_sample_train):
    '''Return percentage of accident'''
    seg_count=count_segments(seg_table_name)
    num_pop=calculate_num_population(seg_count,time_resolution,label_start,label_end)
    
    return pre_sample_train.accidents.count()/num_pop


        
