--- Generates features related to the number of accidents in the past x hours

--- Schema: features_ongevallen_agg

--- Tables
---		A table is created for each of the segmentation schemes





create schema if not exists features_ongevallen_agg;
drop table if exists features_ongevallen_agg.ongevallen_feature_ten_km_one_hour;
drop table if exists features_ongevallen_agg.ongevallen_feature_five_km_one_hour;
drop table if exists features_ongevallen_agg.ongevallen_feature_fifteen_km_one_hour;
drop table if exists features_ongevallen_agg.ongevallen_feature_ten_km_thirty_min;
drop table if exists features_ongevallen_agg.ongevallen_feature_five_km_thirty_min;
drop table if exists features_ongevallen_agg.ongevallen_feature_fifteen_km_thirty_min;

/*
 * CREATING THE TABLE FOR 
 * ONE HOUR
 * TEN KM
 */
create table features_ongevallen_agg.ongevallen_feature_ten_km_one_hour(
	hectokey_merged varchar not null,
	datetime timestamp not null,
	accidents_past_1_num numeric, 
	accidents_past_2_num numeric,
	accidents_past_4_num numeric,
	accidents_past_8_num numeric,
	accidents_past_16_num numeric
);
insert into features_ongevallen_agg.ongevallen_feature_ten_km_one_hour
with times as (
	select datetime, hectokey_merged
	from generate_series((select date_trunc('hour', min(datetime)) from rws_clean.ongevallen), --change truncation
						 (select date_trunc('hour', max(datetime)) from rws_clean.ongevallen), --change truncation
						 '1 hour'::interval) as datetime --change hour interval
	cross join (select distinct hectokey_merged from segmentation.ten_km) as seg --change segmentation
), 
accidents as (
	select 
		seg.hectokey_merged, 
		date_trunc('hour', o.datetime) as datetime,  --change truncation
		count(*) as accident
	from rws_clean.ongevallen as o
	left join segmentation.ten_km as seg --change segmentation
		on o.hectokey = seg.hectokey
	where seg.hectokey_merged is not null --change if need side roads
	group by seg.hectokey_merged, date_trunc('hour', o.datetime) --change truncation	
), 
times_accidents as (
	select 
		t.hectokey_merged,
		t.datetime,
		case 
			when a.accident is null then 0
			else a.accident
		end as accident
	from times as t
	left join accidents as a
		on t.datetime = a.datetime
			and t.hectokey_merged = a.hectokey_merged
)
select 
	hectokey_merged,
	datetime,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 1 preceding and 1 preceding) AS accidents_past_1,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 2 preceding and 1 preceding) AS accidents_past_2,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 4 preceding and 1 preceding) AS accidents_past_4,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 8 preceding and 1 preceding) AS accidents_past_8,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 16 preceding and 1 preceding) AS accidents_past_16
from times_accidents;

/*
 * CREATING THE TABLE FOR 
 * ONE HOUR
 * FIVE KM
 */
create table features_ongevallen_agg.ongevallen_feature_five_km_one_hour(
	hectokey_merged varchar not null,
	datetime timestamp not null,
	accidents_past_1_num numeric, 
	accidents_past_2_num numeric,
	accidents_past_4_num numeric,
	accidents_past_8_num numeric,
	accidents_past_16_num numeric
);
insert into features_ongevallen_agg.ongevallen_feature_five_km_one_hour
with times as (
	select datetime, hectokey_merged
	from generate_series((select date_trunc('hour', min(datetime)) from rws_clean.ongevallen), --change truncation
						 (select date_trunc('hour', max(datetime)) from rws_clean.ongevallen), --change truncation
						 '1 hour'::interval) as datetime --change hour interval
	cross join (select distinct hectokey_merged from segmentation.five_km) as seg --change segmentation
), 
accidents as (
	select 
		seg.hectokey_merged, 
		date_trunc('hour', o.datetime) as datetime,  --change truncation
		count(*) as accident
	from rws_clean.ongevallen as o
	left join segmentation.five_km as seg --change segmentation
		on o.hectokey = seg.hectokey
	where seg.hectokey_merged is not null --change if need side roads
	group by seg.hectokey_merged, date_trunc('hour', o.datetime) --change truncation	
), 
times_accidents as (
	select 
		t.hectokey_merged,
		t.datetime,
		case 
			when a.accident is null then 0
			else a.accident
		end as accident
	from times as t
	left join accidents as a
		on t.datetime = a.datetime
			and t.hectokey_merged = a.hectokey_merged
)
select 
	hectokey_merged,
	datetime,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 1 preceding and 1 preceding) AS accidents_past_1,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 2 preceding and 1 preceding) AS accidents_past_2,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 4 preceding and 1 preceding) AS accidents_past_4,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 8 preceding and 1 preceding) AS accidents_past_8,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 16 preceding and 1 preceding) AS accidents_past_16
from times_accidents;

/*
 * CREATING THE TABLE FOR 
 * ONE HOUR
 * FIFTEEN KM
 */
create table features_ongevallen_agg.ongevallen_feature_fifteen_km_one_hour(
	hectokey_merged varchar not null,
	datetime timestamp not null,
	accidents_past_1_num numeric, 
	accidents_past_2_num numeric,
	accidents_past_4_num numeric,
	accidents_past_8_num numeric,
	accidents_past_16_num numeric
);
insert into features_ongevallen_agg.ongevallen_feature_fifteen_km_one_hour
with times as (
	select datetime, hectokey_merged
	from generate_series((select date_trunc('hour', min(datetime)) from rws_clean.ongevallen), --change truncation
						 (select date_trunc('hour', max(datetime)) from rws_clean.ongevallen), --change truncation
						 '1 hour'::interval) as datetime --change hour interval
	cross join (select distinct hectokey_merged from segmentation.fifteen_km) as seg --change segmentation
), 
accidents as (
	select 
		seg.hectokey_merged, 
		date_trunc('hour', o.datetime) as datetime,  --change truncation
		count(*) as accident
	from rws_clean.ongevallen as o
	left join segmentation.fifteen_km as seg --change segmentation
		on o.hectokey = seg.hectokey
	where seg.hectokey_merged is not null --change if need side roads
	group by seg.hectokey_merged, date_trunc('hour', o.datetime) --change truncation	
), 
times_accidents as (
	select 
		t.hectokey_merged,
		t.datetime,
		case 
			when a.accident is null then 0
			else a.accident
		end as accident
	from times as t
	left join accidents as a
		on t.datetime = a.datetime
			and t.hectokey_merged = a.hectokey_merged
)
select 
	hectokey_merged,
	datetime,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 1 preceding and 1 preceding) AS accidents_past_1,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 2 preceding and 1 preceding) AS accidents_past_2,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 4 preceding and 1 preceding) AS accidents_past_4,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 8 preceding and 1 preceding) AS accidents_past_8,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 16 preceding and 1 preceding) AS accidents_past_16
from times_accidents;

/*
 * 
 * THIRTY MINUTE CHUNK STARTS HERE
 * 
 */

/*
 * CREATING THE TABLE FOR 
 * THIRTY MINUTES
 * TEN KM
 */
create table features_ongevallen_agg.ongevallen_feature_ten_km_thirty_min(
	hectokey_merged varchar not null,
	datetime timestamp not null,
	accidents_past_1_num numeric, 
	accidents_past_2_num numeric,
	accidents_past_4_num numeric,
	accidents_past_8_num numeric,
	accidents_past_16_num numeric
);
insert into features_ongevallen_agg.ongevallen_feature_ten_km_thirty_min
with times as (
	select datetime, hectokey_merged
	from generate_series((select date_trunc('hour', min(datetime)) + date_part('minute', min(datetime))::int / 30 * interval '30 min' from rws_clean.ongevallen), --change truncation
						 (select date_trunc('hour', max(datetime)) + date_part('minute', max(datetime))::int / 30 * interval '30 min' from rws_clean.ongevallen), --change truncation
						 '30 min'::interval) as datetime --change hour interval
	cross join (select distinct hectokey_merged from segmentation.ten_km) as seg --change segmentation
), 
accidents as (
	select 
		seg.hectokey_merged, 
		date_trunc('hour', datetime) + date_part('minute', datetime)::int / 30 * interval '30 min' as datetime,  --change truncation
		count(*) as accident
	from rws_clean.ongevallen as o
	left join segmentation.ten_km as seg --change segmentation
		on o.hectokey = seg.hectokey
	where seg.hectokey_merged is not null --change if need side roads
	group by seg.hectokey_merged, date_trunc('hour', datetime) + date_part('minute', datetime)::int / 30 * interval '30 min' --change truncation	
), 
times_accidents as (
	select 
		t.hectokey_merged,
		t.datetime,
		case 
			when a.accident is null then 0
			else a.accident
		end as accident
	from times as t
	left join accidents as a
		on t.datetime = a.datetime
			and t.hectokey_merged = a.hectokey_merged
)
select 
	hectokey_merged,
	datetime,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 1 preceding and 1 preceding) AS accidents_past_1,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 2 preceding and 1 preceding) AS accidents_past_2,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 4 preceding and 1 preceding) AS accidents_past_4,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 8 preceding and 1 preceding) AS accidents_past_8,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 16 preceding and 1 preceding) AS accidents_past_16
from times_accidents;

/*
 * CREATING THE TABLE FOR 
 * ONE HOUR
 * FIVE KM
 */
create table features_ongevallen_agg.ongevallen_feature_five_km_thirty_min(
	hectokey_merged varchar not null,
	datetime timestamp not null,
	accidents_past_1_num numeric, 
	accidents_past_2_num numeric,
	accidents_past_4_num numeric,
	accidents_past_8_num numeric,
	accidents_past_16_num numeric
);
insert into features_ongevallen_agg.ongevallen_feature_five_km_thirty_min
with times as (
	select datetime, hectokey_merged
	from generate_series((select date_trunc('hour', min(datetime)) + date_part('minute', min(datetime))::int / 30 * interval '30 min' from rws_clean.ongevallen), --change truncation
						 (select date_trunc('hour', max(datetime)) + date_part('minute', max(datetime))::int / 30 * interval '30 min' from rws_clean.ongevallen), --change truncation
						 '30 min'::interval) as datetime --change hour interval
	cross join (select distinct hectokey_merged from segmentation.five_km) as seg --change segmentation
), 
accidents as (
	select 
		seg.hectokey_merged, 
		date_trunc('hour', datetime) + date_part('minute', datetime)::int / 30 * interval '30 min' as datetime,  --change truncation
		count(*) as accident
	from rws_clean.ongevallen as o
	left join segmentation.five_km as seg --change segmentation
		on o.hectokey = seg.hectokey
	where seg.hectokey_merged is not null --change if need side roads
	group by seg.hectokey_merged, date_trunc('hour', datetime) + date_part('minute', datetime)::int / 30 * interval '30 min' --change truncation	
), 
times_accidents as (
	select 
		t.hectokey_merged,
		t.datetime,
		case 
			when a.accident is null then 0
			else a.accident
		end as accident
	from times as t
	left join accidents as a
		on t.datetime = a.datetime
			and t.hectokey_merged = a.hectokey_merged
)
select 
	hectokey_merged,
	datetime,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 1 preceding and 1 preceding) AS accidents_past_1,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 2 preceding and 1 preceding) AS accidents_past_2,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 4 preceding and 1 preceding) AS accidents_past_4,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 8 preceding and 1 preceding) AS accidents_past_8,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 16 preceding and 1 preceding) AS accidents_past_16
from times_accidents;

/*
 * CREATING THE TABLE FOR 
 * THIRTY MINUTES
 * FIFTEEN KM
 */
create table features_ongevallen_agg.ongevallen_feature_fifteen_km_thirty_min(
	hectokey_merged varchar not null,
	datetime timestamp not null,
	accidents_past_1_num numeric, 
	accidents_past_2_num numeric,
	accidents_past_4_num numeric,
	accidents_past_8_num numeric,
	accidents_past_16_num numeric
);
insert into features_ongevallen_agg.ongevallen_feature_fifteen_km_thirty_min
with times as (
	select datetime, hectokey_merged
	from generate_series((select date_trunc('hour', min(datetime)) + date_part('minute', min(datetime))::int / 30 * interval '30 min' from rws_clean.ongevallen), --change truncation
						 (select date_trunc('hour', max(datetime)) + date_part('minute', max(datetime))::int / 30 * interval '30 min' from rws_clean.ongevallen), --change truncation
						 '30 min'::interval) as datetime --change hour interval
	cross join (select distinct hectokey_merged from segmentation.fifteen_km) as seg --change segmentation
), 
accidents as (
	select 
		seg.hectokey_merged, 
		date_trunc('hour', datetime) + date_part('minute', datetime)::int / 30 * interval '30 min' as datetime,  --change truncation
		count(*) as accident
	from rws_clean.ongevallen as o
	left join segmentation.fifteen_km as seg --change segmentation
		on o.hectokey = seg.hectokey
	where seg.hectokey_merged is not null --change if need side roads
	group by seg.hectokey_merged, date_trunc('hour', datetime) + date_part('minute', datetime)::int / 30 * interval '30 min' --change truncation	
), 
times_accidents as (
	select 
		t.hectokey_merged,
		t.datetime,
		case 
			when a.accident is null then 0
			else a.accident
		end as accident
	from times as t
	left join accidents as a
		on t.datetime = a.datetime
			and t.hectokey_merged = a.hectokey_merged
)
select 
	hectokey_merged,
	datetime,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 1 preceding and 1 preceding) AS accidents_past_1,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 2 preceding and 1 preceding) AS accidents_past_2,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 4 preceding and 1 preceding) AS accidents_past_4,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 8 preceding and 1 preceding) AS accidents_past_8,
	sum(accident) over (partition by hectokey_merged 
						order by datetime 
						rows between 16 preceding and 1 preceding) AS accidents_past_16
from times_accidents;