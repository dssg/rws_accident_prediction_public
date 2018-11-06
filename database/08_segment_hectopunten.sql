--- This script clusters the hectopunten that are located on the main highways (also called main carriageways) using a 
--- simple segementation algorithm that splits the hectopunten evenly every 10 km 
---	(of course road segments at the end and beginning of a road may be less than 10 km)

--- Schema: segmentation

--- Tables

--- 	ten_km: shows the hectokey of each hectopunten, and the name of the segment it is assigned to (hectokey_merged) 
---				when trying to divide in 10km segments
---		ten_km_ws: assigns each segment to the nearest weather station
---     repeat for five_km and fifteen_km

create schema if not exists segmentation;

drop table if exists segmentation.ten_km;
drop table if exists segmentation.ten_km_ws;
drop table if exists segmentation.five_km;
drop table if exists segmentation.five_km_ws;
drop table if exists segmentation.fifteen_km;
drop table if exists segmentation.fifteen_km_ws;

/* Creating aggregation of hectokeys to 10km stretches
 * hectokey is foreign key to rws_clean.hectopunten
 * hectokey_merged is the new aggregation
 * */
create table segmentation.ten_km(
	hectokey varchar not null,
	hectokey_merged varchar not null
);

insert into segmentation.ten_km
with ranking as 
(select 
	hectokey,
	road_num, 
	hecto, 
	-- this is the rank (aka number of row) within the group of road number, direction, and type ordered by hectopoint
	rank() over (partition by road_num, road_type, road_dir order by hecto asc)
from rws_clean.unique_hectopunten
where road_type is null) --add  where clause if you want all roads included)
select
	hectokey,
	concat(road_num, 
			'.', 
			-- rounding rank allows us to group 100 hectopoints together 
				-- ie, take everything with rank 0 to 100 and make it a group
			round(rank/100) * 100, --you can change divisor and multiplier to max num of hect per group
			substring(hectokey from '%#".[a-zA-Z]+.[A-Za-z]*#"' for '#')) as hectokey_merged
from ranking;

create table segmentation.ten_km_ws(
	hectokey_merged varchar unique not null,
	weather_station decimal
);
insert into segmentation.ten_km_ws
select 
	ten.hectokey_merged, 
	-- currently using mode (aka the most common weather station in the segment), could be changed
	mode() within group (order by weather_station) as weather_station
from segmentation.ten_km as ten
left join rws_clean.unique_hectopunten as h
	on h.hectokey = ten.hectokey
group by hectokey_merged;


/* Creating aggregation of hectokeys to 5km stretches
 * hectokey is foreign key to rws_clean.hectopunten
 * hectokey_merged is the new aggregation
 * */
create table segmentation.five_km(
	hectokey varchar not null,
	hectokey_merged varchar not null
);

insert into segmentation.five_km
with ranking as 
(select 
	hectokey,
	road_num, 
	hecto, 
	rank() over (partition by road_num, road_type, road_dir order by hecto asc)
from rws_clean.unique_hectopunten
where road_type is null) --add  where clause if you want all roads included)
select
	hectokey,
	concat(road_num, 
			'.', 
			round(rank/50) * 50, --change divisor and multiplier to max num of hect per group
			substring(hectokey from '%#".[a-zA-Z]+.[A-Za-z]*#"' for '#')) as hectokey_merged
from ranking;

create table segmentation.five_km_ws(
	hectokey_merged varchar unique not null,
	weather_station decimal
);
insert into segmentation.five_km_ws
select 
	five.hectokey_merged, 
	mode() within group (order by weather_station) as weather_station
from segmentation.five_km as five
left join rws_clean.unique_hectopunten as h
	on h.hectokey = five.hectokey
group by hectokey_merged;

/* Creating aggregation of hectokeys to 15km stretches
 * hectokey is foreign key to rws_clean.hectopunten
 * hectokey_merged is the new aggregation
 * */
create table segmentation.fifteen_km(
	hectokey varchar not null,
	hectokey_merged varchar not NULL
);
insert into segmentation.fifteen_km
with ranking as 
(select 
	hectokey,
	road_num, 
	hecto, 
	rank() over (partition by road_num, road_type, road_dir order by hecto asc)
from rws_clean.unique_hectopunten
where road_type is null) --add  where clause if you want all roads included)
select
	hectokey,
	concat(road_num, 
			'.', 
			round(rank/150) * 150, --change divisor and multiplier to max num of hect per group
			substring(hectokey from '%#".[a-zA-Z]+.[A-Za-z]*#"' for '#')) as hectokey_merged
from ranking;

create table segmentation.fifteen_km_ws(
	hectokey_merged varchar unique not null,
	weather_station decimal
);
insert into segmentation.fifteen_km_ws
select 
	fifteen.hectokey_merged, 
	mode() within group (order by weather_station) as weather_station
from segmentation.fifteen_km as fifteen
left join rws_clean.unique_hectopunten as h
	on h.hectokey = fifteen.hectokey
group by hectokey_merged;