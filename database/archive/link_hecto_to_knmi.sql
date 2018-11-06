select datetime, h.*, rws_clean.knmi.avg_wind_speed_hr, temp, sun_duration, precip_duration, observability, mist, rain, snow, thunderstorm, ice
from rws_clean.hectopunten as h
inner join rws_clean.knmi on h.weather_station::int4 = rws_clean.knmi.station_id;
