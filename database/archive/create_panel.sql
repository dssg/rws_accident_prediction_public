-- this joins ongeval to hectopunten through the ongeval_hectopunten link
with panel_staging_0 as (
select o.datetime,date_trunc('hour', o.datetime) as datetime_rounded,h.weather_station,
o.ongekey, h.hectoyearkey, 1 as acccident, h.num_lanes_max
from rws_clean.ongevallen as o
left join rws_clean.ong_hect_link as l
on o.ongekey = l.ongekey
left join rws_clean.hectopunten as h
on l.hectoyearkey = h.hectoyearkey),
-- panel_staging_1 connects panel with knmi_features based on weather station
-- and rounded time
panel_staging_1 as (
select panel_staging_0.*, 
kf.avg_wind_speed_hr
-- add features from knmi_features here.
from panel_staging_0 left join rws_clean.knmi_features as kf
on panel_staging_0.datetime_rounded = kf.datetime and panel_staging_0.weather_station = kf.weather_station
),
-- panel_staging_2 is for extracting temporal features.
panel_staging_2 as (
select *,
extract(hour from panel_staging_1.datetime)::int as hour,
extract(month from panel_staging_1.datetime)::int as month,
extract(isodow from panel_staging_1.datetime)::int as day_of_week --monday is 1
from panel_staging_1
)
-- If we wanted flow we would need panel_staging_3 so on and so forth.
select * from panel_staging_2 
;


