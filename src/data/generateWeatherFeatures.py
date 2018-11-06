from WeatherFeatureFactory import WeatherFeatureFactory, weather_driver
import pandas as pd
from utils.misc_utils import connect_rds


# Sample
feature_dict_ago = {
    'columns':['temp', 'avg_wind_speed_hr', 'air_pressure', 'humidity', 'sun_duration', 'precip_duration', 'observability', 'cloud_cover', 'hourly_rain'],
    'hours': [1,2,3,4]
}

feature_dict_agg = {
    'columns':['temp', 'avg_wind_speed_hr', 'air_pressure', 'humidity', 'sun_duration', 'precip_duration', 'observability', 'cloud_cover', 'hourly_rain'],
    'hours': [2,4,8,16],
    'agg_funcs': [np.mean, pd.np.nanmin, pd.np.nanmax]
}

feature_dict_last = {
    'columns':['mist', 'rain', 'snow', 'thunderstorm', 'ice']
}

feature_dict_rain_intensity = {
    'columns':['hourly_rain'],
    'divisors':['precip_duration']
}    

feature_dict_discretize = {
    'columns':['temp', 'avg_wind_speed_hr', 'air_pressure', 'humidity', 'sun_duration', 'precip_duration', 'observability', 'cloud_cover', 'hourly_rain'],
    'nums':[2, 3, 4, 5]
}


feature_config = {
        'feature_weather_x_hr_ago_grid': feature_dict_ago,
        'feature_weather_x_hr_agg_grid': feature_dict_agg,
        'feature_weather_last_event_grid': feature_dict_last,
        'feature_weather_rain_intensity_grid': feature_dict_rain_intensity,
        # discretized removed because code is buggy but also because defining discrete blocks at this 
        # point in pipeline leads to information leakage
        #'feature_weather_discretize_grid':feature_dict_discretize,
        'feature_weather_last_event_grid':feature_dict_last
}


conn = connect_rds()
weather_driver('rws_clean.knmi', feature_config, conn, 'features_knmi_agg', False)
conn.close()