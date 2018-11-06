drop table rws_clean.hectopunten;

create table rws_clean.unique_hectopunten(
	key varchar not null,
	hecto decimal not null,
	road_direction varchar,
	road_type varchar,
	lat decimal not null,
	long decimal not null,
	city varchar,
	road_name_letter varchar,
	road_number varchar,
	weather_station varchar,
	num_lanes_max int,
	num_lanes_min int,
	num_lanes_desc varchar
);

with u
as (select *, 
	row_number() over(partition by h.key, h.pos_tv_wol order by h.jaar desc) as ranking
	from rws_schema.hectopunten as h)
insert into rws_clean.unique_hectopunten	
select 
	key,
	cast(replace(hecto, ',', '.') as decimal),
	pos_tv_wol as road_direction,
	dvk_letter as road_type,
	cast(replace(coords_x2, ',', '.') as decimal) as lat,
	cast(replace(coords_x1, ',', '.') as decimal) as lat,
	wpsnaamnen as city,
	a_n_weg as road_letter_name,
	weg as road_number, --maybe wegnummer?
	weerstation as weather_station,
	rijstrook_aantal_max as num_lanes_max,
	rijstrook_aantal_min as num_lanes_min,
	rijstrook as num_lanes_desc
from u
where u.ranking = 1;