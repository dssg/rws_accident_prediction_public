/**************************************
 * NOTE: Segmentation does not properly segment side roads and on/off ramps
 * 	     and shouldn't be used on hectokeys that contain a dvk_letter.
 * ************************************/

create schema if not exists segmentation;

drop table if exists segmentation.ten_km;
drop table if exists segmentation.ten_km_ws;

drop table if exists segmentation.five_km;
drop table if exists segmentation.five_km_ws;

drop table if exists segmentation.three_km;
drop table if exists segmentation.three_km_ws;

drop table if exists segmentation.two_km;
drop table if exists segmentation.two_km_ws;

drop table if exists segmentation.one_km;
drop table if exists segmentation.one_km_ws;

/**************************************
 *  Create Segmentation for ten km 
 * 
 * Creating aggregation of hectokeys to 10km stretches
 * hectokey is foreign key to rws_clean.hectopunten
 * hectokey_merged is the new aggregation
 * 
 * NOTE: side roads (those where dvk_letter is not null) have much shorter aggregation levels,
 * 		 as they don't have 10km of road in a row
 * ************************************/
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
	rank() over (partition by road_num, road_type, road_dir order by hecto asc)
from rws_clean.unique_hectopunten) --add  where clause if you want all roads included)
select
	hectokey,
	concat(road_num, 
			'.', 
			round(rank/100) * 100, --change divisor and multiplier to max num of hect per group
			substring(hectokey from '%#".[a-zA-Z]+.[A-Za-z]*#"' for '#')) as hectokey_merged
from ranking;

create table segmentation.ten_km_ws(
	hectokey_merged varchar unique not null,
	weather_station decimal
);
insert into segmentation.ten_km_ws
select 
	ten.hectokey_merged, 
	mode() within group (order by weather_station) as weather_station
from segmentation.ten_km as ten
left join rws_clean.unique_hectopunten as h
	on h.hectokey = ten.hectokey
group by hectokey_merged;


/**************************************
 *  Create Segmentation for five km 
 * ************************************/
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
from rws_clean.unique_hectopunten) --add  where clause if you want all roads included)
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



/**************************************
 *  Create Segmentation for three km 
 * ************************************/
create table segmentation.three_km(
	hectokey varchar not null,
	hectokey_merged varchar not null
);

insert into segmentation.three_km
with ranking as 
(select 
	hectokey,
	road_num, 
	hecto, 
	rank() over (partition by road_num, road_type, road_dir order by hecto asc)
from rws_clean.unique_hectopunten) --add  where clause if you want all roads included)
select
	hectokey,
	concat(road_num, 
			'.', 
			round(rank/30) * 30, --change divisor and multiplier to max num of hect per group
			substring(hectokey from '%#".[a-zA-Z]+.[A-Za-z]*#"' for '#')) as hectokey_merged
from ranking;

create table segmentation.three_km_ws(
	hectokey_merged varchar unique not null,
	weather_station decimal
);
insert into segmentation.three_km_ws
select 
	three.hectokey_merged, 
	mode() within group (order by weather_station) as weather_station
from segmentation.three_km as three
left join rws_clean.unique_hectopunten as h
	on h.hectokey = three.hectokey
group by hectokey_merged;

/**************************************
 *  Create Segmentation for two km 
 * ************************************/
create table segmentation.two_km(
	hectokey varchar not null,
	hectokey_merged varchar not null
);

insert into segmentation.two_km
with ranking as 
(select 
	hectokey,
	road_num, 
	hecto, 
	rank() over (partition by road_num, road_type, road_dir order by hecto asc)
from rws_clean.unique_hectopunten) --add  where clause if you want all roads included)
select
	hectokey,
	concat(road_num, 
			'.', 
			round(rank/20) * 20, --change divisor and multiplier to max num of hect per group
			substring(hectokey from '%#".[a-zA-Z]+.[A-Za-z]*#"' for '#')) as hectokey_merged
from ranking;

create table segmentation.two_km_ws(
	hectokey_merged varchar unique not null,
	weather_station decimal
);
insert into segmentation.two_km_ws
select 
	two.hectokey_merged, 
	mode() within group (order by weather_station) as weather_station
from segmentation.two_km as two
left join rws_clean.unique_hectopunten as h
	on h.hectokey = two.hectokey
group by hectokey_merged;

/**************************************
 *  Create Segmentation for one km 
 * ************************************/
create table segmentation.one_km(
	hectokey varchar not null,
	hectokey_merged varchar not null
);

insert into segmentation.one_km
with ranking as 
(select 
	hectokey,
	road_num, 
	hecto, 
	rank() over (partition by road_num, road_type, road_dir order by hecto asc)
from rws_clean.unique_hectopunten) --add  where clause if you want all roads included)
select
	hectokey,
	concat(road_num, 
			'.', 
			round(rank/10) * 10, --change divisor and multiplier to max num of hect per group
			substring(hectokey from '%#".[a-zA-Z]+.[A-Za-z]*#"' for '#')) as hectokey_merged
from ranking;

create table segmentation.one_km_ws(
	hectokey_merged varchar unique not null,
	weather_station decimal
);
insert into segmentation.one_km_ws
select 
	one.hectokey_merged, 
	mode() within group (order by weather_station) as weather_station
from segmentation.one_km as one
left join rws_clean.unique_hectopunten as h
	on h.hectokey = one.hectokey
group by hectokey_merged;