create schema if not exists rws_clean;

create schema if not exists features_hectopunten_agg;
create schema if not exists features_knmi_agg;
create schema if not exists features_speedflow_agg;

create schema if not exists rws_experiment;
create schema if not exists model_spines;

create schema if not exists segmentation;


/* Dropping existing tables to create from scratch */
drop table if exists rws_clean.hectopunten cascade;
drop materialized view if exists rws_clean.hectopunten_rollup;
drop table if exists rws_clean.knmi;
drop table if exists rws_clean.ongevallen;
drop table if exists rws_clean.network;
drop view if exists rws_clean.temp_hectopunten;

/* Cleaning Hectopunten 
 * hectoyearkey is the unique identifier: a specific point measured for a specific year
 * hectokey is the identifier for a specific point
 * road type is null for main highways, letters refer to sideroads, entry exit points, etc
 * weather_station is foreign key for weather data*/
create table rws_clean.hectopunten(
	hectokey varchar not null,
	hectoyearkey varchar,
	year decimal,
    hecto decimal not null,
    road_dir varchar,
    road_type varchar,
    lat decimal not null,
    long decimal not null,
    city varchar,
    road_prefix_letter varchar,
    road_num int,
    weather_station int,
    num_lanes_max int,
    num_lanes_min int,
    num_lanes_desc varchar,
    district varchar, 
	speedlim_6_19 decimal,
	speedlim_19_6 decimal,
	on_ramp varchar,
	off_ramp varchar,
	tunnel varchar,
	asphalt varchar, 
	lighting varchar, 
	curve decimal,
	tilt decimal
);

--taking all hectopoints (and every year they come up) for all in RWS definition of utrecht
-- Note: hectokey is what we use to uniquely identify a hectopunten.
insert into rws_clean.hectopunten 
with h as 
(select
    concat(key, '.', pos_tv_wol) as hectokey,
    key_jaar as hectoyearkey,
    jaar as year,
    cast(replace(hecto, ',', '.') as decimal) as hecto,
    pos_tv_wol as road_dir,
    dvk_letter as road_type,
    cast(replace(coords_x2, ',', '.') as decimal) as lat,
    cast(replace(coords_x1, ',', '.') as decimal) as long,
    wpsnaamnen as city,
    a_n_weg as road_prefix_letter,
    weg::int as road_num,
    weerstation::int as weather_station,
    rijstrook_aantal_max as num_lanes_max,
    rijstrook_aantal_min as num_lanes_min,
    rijstrook as v,
    dienstnaam as district, 
	max_snelheid_6_19::decimal, 
	max_snelheid_19_6::decimal,
	convergentie,
	divergentie,
	kruispunt,
	verharding,
	verlichting,
	replace(boogstraal, ',', '.')::decimal, 
	replace(dwarshelling, ',', '.')::decimal
from rws_raw.hectopunten)
select * 
from h
where --hecto is between the range for the road that is included in utrecht
	(road_num = 1 and hecto >= 26.1 and hecto <= 51.6) 
	or (road_num = 2 and hecto >= 38 and hecto <= 94.3)
	or (road_num = 12 and hecto >= 36.6 and hecto <= 103.3)
	or (road_num = 27 and hecto >= 50.4 and hecto <= 100.3)
	or (road_num = 28 and hecto >= 0 and hecto <= 33.7)
	and road_type is null
order by 
	hectokey, 
	year desc;
		
/*
 *Creating a table with the hectopunten rollup
 *Goal is to have a table that includes all the hectopunten with data for each year of 2015, 2016, and 2017
 *The data will not necessarily be ~from~ the year of interest, but will be the most recent observation 
 *	of the hectopoint in the past. So an obs to link to 2016 can actually be data from 2015 but not 2017
 */
create materialized view rws_clean.hectopunten_rollup as 
((select distinct on (hectokey)
	2015 as rollup_year, h.*
from rws_clean.hectopunten as h
where year < 2016
order by hectokey, year desc)
union
(select distinct on (hectokey)
	 2016 as rollup_year, h.*
from rws_clean.hectopunten as h
where year < 2017
order by hectokey, year desc)
union 
(select distinct on (hectokey)
	2017 as rollup_year, h.*
from rws_clean.hectopunten as h
where year < 2018
order by hectokey, year desc));

/* Create a list of hectopunten with simple location metadata to use when attempting to locate unique ids
 * Contains no data that ~should~ vary with year.*/
create materialized view rws_clean.unique_hectopunten as
select distinct on (hectokey)
	hectokey, 
	road_num,
	road_type,
	road_dir, 
	lat,
	long,
	hecto, 
	weather_station
from rws_clean.hectopunten
order by hectokey, year;

/*Create a temporary view for hectoputen linked to  WORKING weather stations
 * Note: we have no data from weather station 265 (therefore we need to impute data with another weather station)
 * Find the closest weather station for the hectopoints that link to 265
 */
create view rws_clean.temp_hectopunten as 


select  
	h.hectokey,
	stn."STN",
	stn.dist,
	h.year,
	h.weather_station
from rws_clean.hectopunten as h	
left join lateral 
	--left join lateral to find the closest station for each hectopunten
	(select 
		h.hectokey, 
		"STN",
		h.weather_station, 
		ST_Distance(ST_MakePoint(h.lat,h.long),ST_MakePoint("LAT(north)","LON(east)")) as dist  
	from rws_raw.knmi_station
	where "STN" != 265 
	order by  ST_Distance(ST_MakePoint(h.lat,h.long),ST_MakePoint("LAT(north)","LON(east)"))  asc limit 1) 
	as stn
on true
where h.weather_station = 265;

/*Update rws_clean.hectopunten.weather_station */
update rws_clean.hectopunten
set weather_station = t."STN"
from rws_clean.temp_hectopunten as t
where rws_clean.hectopunten.weather_station = 265
	and t.hectokey = rws_clean.hectopunten.hectokey;

/*Drop the view now that we've fixed hectopunten*/
drop view rws_clean.temp_hectopunten;

/* Cleaning Ongevallen 
 * ongekey is the unique identifier generated, road#.hecto#.(direction or roadtype).direction.date
 * hectokey is the identifier for the location, road#.hecto#.(direction or roadtype).direction */
create table rws_clean.ongevallen(
	ongekey varchar not null unique,
    hectokey varchar not null, 
    datetime timestamp without time zone,  
    long decimal not null, 
    lat decimal not null,
    hecto decimal not null, 
    inc_severity varchar, 
    road_num int, 
    road_dir varchar, 
    road_type varchar
);

--selecting for years 2016 on, for hectopoints included in the cleaned hecto data
insert into rws_clean.ongevallen
select 
	concat(key, '.', pos_tv_wol, '.', datumtijd) as ongekey,
	concat(key, '.', pos_tv_wol) as hectokey,
	datumtijd as datetime,
	cast(replace(coords_x, ',', '.') as decimal) as long,
	cast(replace(coords_y, ',', '.') as decimal) as lat,
	cast(replace(hecto, ',', '.') as decimal),
	afloop as inc_severity,
	cast(weg as int) as road_num,
	pos_tv_wol as road_dir,
	dvk_letter as road_type
from rws_raw.ongevallen
where 
	jaar::int > 2015 
	and concat(key, '.', pos_tv_wol) in (select hectokey from rws_clean.hectopunten); 

/* Cleaning KNMI
 * weather_station links to hectopunten */
create table rws_clean.knmi(
	weather_station int not null,
	datetime timestamp without time zone,
	avg_wind_speed_hr decimal,
	hi_wind_speed_hr decimal,
	temp decimal,
	air_pressure decimal,
	humidity decimal,
	sun_duration decimal,
	precip_duration decimal,
	observability decimal,
	cloud_cover decimal,
	hourly_rain decimal,
	mist boolean,
	rain boolean,
	snow boolean,
	thunderstorm boolean,
	ice boolean
);

--selecting
insert into rws_clean.knmi
select 
	"STN" as weather_station,
	("YYYYMMDD" + ("HH")  * INTERVAL '1 hour') as datetime,
	"FH" as avg_wind_speed_hr,
	"FX" as hi_wind_speed_hr,
	"T" as temp,
	"P" as air_pressure,
	"U" as humidity,
	case 
		--multiplying so that units are minutes (interpretability)
		when "SQ" = -1 then 3
		else "SQ" * 6
	end as sun_duration,
	"DR" * 6 as precip_duration,
	"VV" as observability,
	"N" as cloud_cover, --higher = more clouds
	case 
		when "RH" = -1 then 0
		else "RH"
	end as hourly_rain,
	"M" as mist,
	"R" as rain,
	"S" as snow,
	"O" as thunderstorm,
	"Y" as ice
from rws_raw.knmi_data
where 
	"STN" in (select distinct weather_station from rws_clean.hectopunten)
	and ("YYYYMMDD" + ("HH")  * INTERVAL '1 hour') between '2016-01-01 00:00:00' and '2018-04-15 23:00:00';
	