--- Clean the raw tables (e.g. rename columns to English, change column types)
--- Filter the data to only include the Utrecht region
--- Note: this does not include the flow data

--- Schema: Rws_Clean

--- Tables
--- 	hectopunten: contains metadata for each hectopunten.  
---			Note there are multiple rows for each hectopunt as a new observation was recorded each time there was an update to the road
--- 	hectopunten_rollup: contains hectopunten entry for each year
---			e.g. if there were no changes to a road between 2015 and 2016, the hectopunten data for 
---			2016 would be a copy of the data from 2015
---		unique_hectopunten: contains a single entry for each hectopunten with the most recent data

--- 	ongevallen: accident data

--- 	knmi: weather observation over time by weather station


create schema if not exists rws_clean;

-- drop existing tables
DROP materialized VIEW rws_clean.hectopunten_rollup;
DROP materialized VIEW rws_clean.unique_hectopunten;
drop table if exists rws_clean.hectopunten;
drop table if exists rws_clean.knmi;
drop table if exists rws_clean.ongevallen;



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
	on_ramp_convergentie varchar,
	off_ramp_divergentie varchar,
	tunnel varchar,
	asphalt varchar, 
	lighting varchar, 
	curve decimal,
	tilt decimal,
	distance_to_billboard decimal,
	trees_within50m decimal,
	trees_within100m decimal,
	overtaking_allowed varchar,
	distance_to_curvature_category_0_100 decimal,
	distance_to_curvature_category_101_200 decimal,
	distance_to_curvature_category_201_300 decimal,
	distance_to_curvature_category_301_400 decimal,
	distance_to_curvature_category_401_500 decimal,
	asphalt_age int,
	ramp_code varchar,
	advisory_speed decimal,
	max_avg_speed_difference_6_19 decimal,
	max_avg_speed_difference_19_6 decimal,
	dosing_installation_traffic_light varchar,
	distance_from_road_to_sound_barrier decimal,
	lamp_post int,
	compass_direction_degrees decimal,
	compass_direction_category varchar,
	distance_to_on_ramp_convergentie decimal,
	distance_to_off_ramp_divergentie decimal,
	distance_to_bst_code_OPR DECIMAL,
	distance_bst_code_AFR decimal,
	distance_to_railway_crossing decimal,
	distance_to_parking decimal,
	distance_to_intersection decimal,
	distance_to_lane_widening decimal,
	distance_to_lane_narrowing decimal,
	merge_lane_type varchar
);

--taking all hectopoints (and every year they come up) for all in RWS definition of utrecht
-- Note: hectokey is what we use to uniquely identify a hectopunten, but not uniquely identify a row in this table
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
    rijstrook as num_lanes_desc,
    dienstnaam as district, 
	max_snelheid_6_19::decimal, 
	max_snelheid_19_6::decimal,
	convergentie AS on_ramp_convergentie,
	divergentie AS off_ramp_divergentie,
	kruispunt AS tunnel,
	verharding AS asphalt,
	verlichting AS lighting,
	replace(boogstraal, ',', '.')::decimal AS curve, 
	replace(dwarshelling, ',', '.')::decimal AS tilt,
	replace(reclamemast_afstand, ',', '.')::decimal AS distance_to_billboard,
	trees_within50m::int,
	trees_within100m::int,
	inhaalverbod AS overtaking_allowed,
	afstand_boogstraal_categorie_0_100::decimal AS distance_to_curvature_category_0_100,
	afstand_boogstraal_categorie_101_200::decimal AS distance_to_curvature_category_101_200,
	afstand_boogstraal_categorie_201_300::decimal AS distance_to_curvature_category_201_300,
	afstand_boogstraal_categorie_301_400::decimal AS distance_to_curvature_category_301_400,
	afstand_boogstraal_categorie_401_500::decimal AS distance_to_curvature_category_401_500,
	mjpv_verharding_leeftijd::int AS asphalt_age,
	bst_code AS ramp_code,
	adv_snelheid::decimal as advisory_speed,
	max_snelheid_verschil_6_19::decimal AS max_avg_speed_difference_6_19,
	max_snelheid_verschil_19_6::decimal AS max_avg_speed_difference_19_6,
	convergentie_doseerinst AS dosing_installation_traffic_light,
	replace(geluidsbeperking_afstand, ',', '.')::decimal AS distance_from_road_to_sound_barrier,
	lichtmast::int AS lamp_post,
	replace(kompas_graden, ',', '.')::decimal AS compass_direction_degrees,
	kompas AS compass_direction_category,
	afstand_convergentie_type::decimal AS distance_to_on_ramp_convergentie,
	afstand_divergentie_type::decimal AS distance_to_off_ramp_divergentie,
	afstand_bst_code_opr::decimal AS distance_to_bst_code_OPR ,
	afstand_bst_code_afr::decimal AS distance_bst_code_AFR,
	afstand_spoorovergang::decimal AS distance_to_railway_crossing,
	afstand_bst_code_parkeerplaats::decimal AS distance_to_parking,
	afstand_kruispunt::decimal AS distance_to_intersection,
	afstand_rijstrook_verbreding::decimal AS distance_to_lane_widening,
	afstand_rijstrook_versmalling::decimal AS distance_to_lane_narrowing,
	mengstrook AS merge_lane_type
from rws_raw.hectopunten)
select * 
from h
where --it is within the bounds we know for the utrecht region along that road
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
 * Adding a column for data on how many accidents have happened at each point in the past
 * This is rather specific to our daya - this aggregation is pre 2016, and all other data is post, aka no leakage
 */

create table rws_clean.static_hectopunten(
	hectokey varchar not null, 
	accidents_pre2016 NUMERIC,
	dod_accidents_pre2016 numeric, 
	leh_accidents_pre2016 numeric, 
	lov_accidents_pre2016 numeric, 
	lzh_accidents_pre2016 numeric, 
	ums_accidents_pre2016 numeric
);

insert into rws_clean.static_hectopunten
with a as (
	select 
		concat(key, '.', pos_tv_wol) as hectokey,
		count(*) as accidents_pre2016, 
		count(*) filter (where afloop = 'DOD') as dod_accidents_pre2016, 
		count(*) filter (where afloop = 'LEH') as leh_accidents_pre2016,
		count(*) filter (where afloop = 'LOV') as lov_accidents_pre2016,
		count(*) filter (where afloop = 'LZH') as lzh_accidents_pre2016,
		count(*) filter (where afloop = 'UMS') as ums_accidents_pre2016,
	from rws_raw.ongevallen as o
	where 
		o.jaar::int <= '2015' 
		and concat(o.key, '.', o.pos_tv_wol) in (select hectokey from rws_clean.hectopunten)
	group by concat(o.key, '.', o.pos_tv_wol)
)
select 
	h.hectokey,
	case 
		when a.accidents_pre2016 is null then 0
		else a.accidents_pre2016
	end as accidents_pre2016, 
	case 
		when a.dod_accidents_pre2016 is null then 0
		else a.dod_accidents_pre2016
	end as dod_accidents_pre2016,
	case 
		when a.leh_accidents_pre2016 is null then 0
		else a.leh_accidents_pre2016
	end as leh_accidents_pre2016,
	case 
		when a.lov_accidents_pre2016 is null then 0
		else a.lov_accidents_pre2016
	end as lov_accidents_pre2016,
	case 
		when a.lzh_accidents_pre2016 is null then 0
		else a.lzh_accidents_pre2016
	end as lzh_accidents_pre2016,
	case 
		when a.ums_accidents_pre2016 is null then 0
		else a.ums_accidents_pre2016
	end as ums_accidents_pre2016
from (select distinct hectokey from rws_clean.hectopunten) as h
left join a
	on a.hectokey = h.hectokey;
	

ALTER TABLE rws_clean.hectopunten 
	ADD COLUMN accidents_pre2016 NUMERIC,
	ADD COLUMN dod_accidents_pre2016 NUMERIC,
	ADD COLUMN leh_accidents_pre2016 NUMERIC,
	ADD COLUMN lzh_accidents_pre2016 NUMERIC,
	ADD COLUMN ums_accidents_pre2016 NUMERIC;
	
update rws_clean.hectopunten
set accidents_pre2016 = s.accidents_pre2016,
	dod_accidents_pre2016 = s.dod_accidents_pre2016,
	leh_accidents_pre2016 = s.leh_accidents_pre2016,
	lzh_accidents_pre2016 = s.lzh_accidents_pre2016, 
	ums_accidents_pre2016 = s.ums_accidents_pre2016
from rws_clean.static_hectopunten as s
where rws_clean.hectopunten.hectokey = s.hectokey;

DROP TABLE rws_clean.static_hectopunten;
		
/*
 * Creating a table with the hectopunten rollup
 * Goal is to have a table that includes an observation for each year (2015, 2016, 2016) to link features at each time for each hectopoint.
 * The data will not necessarily be ~from~ the year of interest, but will be the most recent observation 
 *	 of the hectopoint in the past. So an obs to link to 2016 can be data from 2015 but not 2017
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

/* Create a unique list of hectopunten with simple location metadata to use when attempting to locate unique ids
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
order by hectokey, YEAR;

/*Create a temporary view for hectoputen linked to  WORKING weather stations
 * Since we have no data from weather station 265 we find the closest station for all those assigned to 265
 */
create view rws_clean.temp_hectopunten as 
select  
	stn.hectokey,
	stn."STN",
	h.weather_station
from rws_clean.hectopunten as h	
left join lateral 
	--left join lateral to find the closest station for each hectopunten
	(select 
		h.hectokey, 
		"STN",
		h.weather_station, 
		ST_Distance(ST_MakePoint(h.lat,h.long),ST_MakePoint("LAT(north)","LON(east)"))  
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
	
	
	
	

/* Cleaning KNMI*/
--selecting for years 2016 on
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
    	--setting to 0, since -1 means 0>RH>0.005, better to round than -1
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

