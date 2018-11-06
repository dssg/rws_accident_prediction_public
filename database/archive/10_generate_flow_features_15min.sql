--- This script generated features that use the speed and flow data already averaged at 1 hour resolution. 
--- The resulting table will be called by the Python pipeline script to use for prediction

--- Schema: features_speedflow_agg

--- Tables
--- 	fifteenmin_hecto_v1: This table aggregates features across lanes for each hectopunt.  
---			The features in this table are used to generate features that are across segments. 

--- 	fifteenmin_ten_km_v2: Uses the fifteenmin_hecto_v1 table to aggregate features across road segments.
---			The segmentation scheme, segmentation.ten_km is used for segment assignments
---			The rws_clean.flow_network table is used to determined speed differences between neighboring loops

--- 	fifteenmin_five_km_v1: aggregates over 5 km segments using segmentation.five_km table

--- 	fifteenmin_three_km_v1: aggregates over 3 km segments using segmentation.three_km table

--- 	fifteenmin_two_km_v1: aggregates over 2 km segments using segmentation.two_km table

--- 	fifteenmin_one_km_v1: aggregates over 1 km segments using segmentation.one_km table


-- generate the flow features table using the data in speedflow

-- first do a group by on hecto
create schema if not exists features_speedflow_agg;

drop table if EXISTS features_speedflow_agg.fifteenmin_hecto_v1;
create table features_speedflow_agg.fifteenmin_hecto_v1(
	end_datetime timestamp without time zone,
	hectokey varchar,
	total_flow_on_hecto decimal,
	avg_flow_on_hecto decimal,
	avg_speed_on_hecto decimal,
	max_flow_across_lanes decimal,
	min_flow_across_lanes decimal,
	max_speed_across_lanes decimal,
	min_speed_across_lanes decimal,
	max_speed_std_across_lanes decimal,
	min_speed_std_across_lanes decimal,
	avg_speed_std_across_lanes decimal
);


insert into features_speedflow_agg.fifteenmin_hecto_v1
select 
	end_datetime,
	hectokey,
	sum(flow_avg_all) as total_flow_on_hecto,
	avg(flow_avg_all) as avg_flow_on_hecto,
	avg(speed_avg_all) as avg_speed_on_hecto,
	max(flow_avg_all) as max_flow_across_lanes,
	min(flow_avg_all) as min_flow_across_lanes,
	max(speed_avg_all) as max_speed_across_lanes,
	min(speed_avg_all) as min_speed_across_lanes,
	max(speed_std_all) as max_speed_std_across_lanes,
	min(speed_std_all) as min_speed_std_across_lanes,
	avg(speed_std_all) AS avg_speed_std_across_lanes
from rws_clean.speedflow_15min
group by hectokey, end_datetime;

-- create additional features
alter TABLE features_speedflow_agg.fifteenmin_hecto_v1
add column max_speed_diff_across_lanes decimal,
ADD COLUMN avg_speed_diff_from_neighbor_loop_ahead DECIMAL,
ADD COLUMN avg_speed_diff_from_neighbor_loop_behind decimal,
ADD COLUMN avg_speed_diff_from_prev_recording decimal;


UPDATE  features_speedflow_agg.fifteenmin_hecto_v1 AS curr
SET avg_speed_diff_from_prev_recording = curr.avg_speed_on_hecto - prev.avg_speed_on_hecto
FROM features_speedflow_agg.fifteenmin_hecto_v1 AS prev
WHERE curr.hectokey = prev.hectokey
and curr.end_datetime = prev.end_datetime + interval '15 minute';

UPDATE features_speedflow_agg.fifteenmin_hecto_v1
SET max_speed_diff_across_lanes = max_speed_across_lanes - min_speed_across_lanes;


UPDATE features_speedflow_agg.fifteenmin_hecto_v1 curr
SET    avg_speed_diff_from_neighbor_loop_ahead = curr.avg_speed_on_hecto - adj.avg_speed_on_hecto
FROM   features_speedflow_agg.fifteenmin_hecto_v1  adj
left join   rws_clean.flow_network link 
ON link.hectokey_ahead = adj.hectokey
WHERE  curr.end_datetime = adj.end_datetime
and link.hectokey = curr.hectokey;

UPDATE features_speedflow_agg.fifteenmin_hecto_v1 curr
SET    avg_speed_diff_from_neighbor_loop_behind = curr.avg_speed_on_hecto - adj.avg_speed_on_hecto
FROM   features_speedflow_agg.fifteenmin_hecto_v1  adj
left join   rws_clean.flow_network link 
ON link.hectokey_behind = adj.hectokey
WHERE  curr.end_datetime = adj.end_datetime
and link.hectokey = curr.hectokey;

-- Now aggregate across segments



-------------------- 10 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.fifteenmin_ten_km_v1;
create table features_speedflow_agg.fifteenmin_ten_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg decimal,
	speed_diffxlanes_maxhecto_maxseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_avgseg decimal,
	speed_avgxlanes_diff_from_loop_behind_avgseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_maxseg decimal,
	speed_avgxlanes_diff_from_loop_behind_maxseg decimal,
	speed_avgxlanes_diff_from_prev_rec_avgseg decimal,
	speed_avgxlanes_diff_from_prev_rec_maxseg decimal,
	flow_avgxlanes_avgseg decimal,
	flow_avgxlanes_maxseg decimal,
	flow_avgxlanes_minseg decimal,
	speed_avgxlanes_avgseg decimal,
	speed_avgxlanes_maxseg decimal,
	speed_avgxlanes_minseg decimal,
	speed_std_avgxlanes_maxseg decimal
);


insert into features_speedflow_agg.fifteenmin_ten_km_v1
select 
	sf.end_datetime,
	seg.hectokey_merged,
	avg(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_avgseg,
	max(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_maxseg,
	avg(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	avg(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	avg(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	avg(avg_flow_on_hecto) AS flow_avgxlanes_avgseg,
	max(avg_flow_on_hecto) AS flow_avgxlanes_maxseg,
	min(avg_flow_on_hecto) AS flow_avgxlanes_minseg,
	avg(avg_speed_on_hecto) AS speed_avgxlanes_avgseg,
	max(avg_speed_on_hecto) AS speed_avgxlanes_maxseg,
	min(avg_speed_on_hecto) AS speed_avgxlanes_minseg,
	max(avg_speed_std_across_lanes) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_hecto_v1 as sf
left join segmentation.ten_km as seg
	on sf.hectokey = seg.hectokey
group by hectokey_merged, end_datetime;



-------------------- 5 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.fifteenmin_five_km_v1;
create table features_speedflow_agg.fifteenmin_five_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg decimal,
	speed_diffxlanes_maxhecto_maxseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_avgseg decimal,
	speed_avgxlanes_diff_from_loop_behind_avgseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_maxseg decimal,
	speed_avgxlanes_diff_from_loop_behind_maxseg decimal,
	speed_avgxlanes_diff_from_prev_rec_avgseg decimal,
	speed_avgxlanes_diff_from_prev_rec_maxseg decimal,
	flow_avgxlanes_avgseg decimal,
	flow_avgxlanes_maxseg decimal,
	flow_avgxlanes_minseg decimal,
	speed_avgxlanes_avgseg decimal,
	speed_avgxlanes_maxseg decimal,
	speed_avgxlanes_minseg decimal,
	speed_std_avgxlanes_maxseg decimal
);


insert into features_speedflow_agg.fifteenmin_five_km_v1
select 
	sf.end_datetime,
	seg.hectokey_merged,
	avg(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_avgseg,
	max(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_maxseg,
	avg(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	avg(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	avg(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	avg(avg_flow_on_hecto) AS flow_avgxlanes_avgseg,
	max(avg_flow_on_hecto) AS flow_avgxlanes_maxseg,
	min(avg_flow_on_hecto) AS flow_avgxlanes_minseg,
	avg(avg_speed_on_hecto) AS speed_avgxlanes_avgseg,
	max(avg_speed_on_hecto) AS speed_avgxlanes_maxseg,
	min(avg_speed_on_hecto) AS speed_avgxlanes_minseg,
	max(avg_speed_std_across_lanes) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_hecto_v1 as sf
left join segmentation.five_km as seg
	on sf.hectokey = seg.hectokey
group by hectokey_merged, end_datetime;



-------------------- 3 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.fifteenmin_three_km_v1;
create table features_speedflow_agg.fifteenmin_three_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg decimal,
	speed_diffxlanes_maxhecto_maxseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_avgseg decimal,
	speed_avgxlanes_diff_from_loop_behind_avgseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_maxseg decimal,
	speed_avgxlanes_diff_from_loop_behind_maxseg decimal,
	speed_avgxlanes_diff_from_prev_rec_avgseg decimal,
	speed_avgxlanes_diff_from_prev_rec_maxseg decimal,
	flow_avgxlanes_avgseg decimal,
	flow_avgxlanes_maxseg decimal,
	flow_avgxlanes_minseg decimal,
	speed_avgxlanes_avgseg decimal,
	speed_avgxlanes_maxseg decimal,
	speed_avgxlanes_minseg decimal,
	speed_std_avgxlanes_maxseg decimal
);


insert into features_speedflow_agg.fifteenmin_three_km_v1
select 
	sf.end_datetime,
	seg.hectokey_merged,
	avg(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_avgseg,
	max(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_maxseg,
	avg(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	avg(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	avg(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	avg(avg_flow_on_hecto) AS flow_avgxlanes_avgseg,
	max(avg_flow_on_hecto) AS flow_avgxlanes_maxseg,
	min(avg_flow_on_hecto) AS flow_avgxlanes_minseg,
	avg(avg_speed_on_hecto) AS speed_avgxlanes_avgseg,
	max(avg_speed_on_hecto) AS speed_avgxlanes_maxseg,
	min(avg_speed_on_hecto) AS speed_avgxlanes_minseg,
	max(avg_speed_std_across_lanes) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_hecto_v1 as sf
left join segmentation.three_km as seg
	on sf.hectokey = seg.hectokey
group by hectokey_merged, end_datetime;





-------------------- 2 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.fifteenmin_two_km_v1;
create table features_speedflow_agg.fifteenmin_two_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg decimal,
	speed_diffxlanes_maxhecto_maxseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_avgseg decimal,
	speed_avgxlanes_diff_from_loop_behind_avgseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_maxseg decimal,
	speed_avgxlanes_diff_from_loop_behind_maxseg decimal,
	speed_avgxlanes_diff_from_prev_rec_avgseg decimal,
	speed_avgxlanes_diff_from_prev_rec_maxseg decimal,
	flow_avgxlanes_avgseg decimal,
	flow_avgxlanes_maxseg decimal,
	flow_avgxlanes_minseg decimal,
	speed_avgxlanes_avgseg decimal,
	speed_avgxlanes_maxseg decimal,
	speed_avgxlanes_minseg decimal,
	speed_std_avgxlanes_maxseg decimal
);


insert into features_speedflow_agg.fifteenmin_two_km_v1
select 
	sf.end_datetime,
	seg.hectokey_merged,
	avg(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_avgseg,
	max(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_maxseg,
	avg(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	avg(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	avg(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	avg(avg_flow_on_hecto) AS flow_avgxlanes_avgseg,
	max(avg_flow_on_hecto) AS flow_avgxlanes_maxseg,
	min(avg_flow_on_hecto) AS flow_avgxlanes_minseg,
	avg(avg_speed_on_hecto) AS speed_avgxlanes_avgseg,
	max(avg_speed_on_hecto) AS speed_avgxlanes_maxseg,
	min(avg_speed_on_hecto) AS speed_avgxlanes_minseg,
	max(avg_speed_std_across_lanes) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_hecto_v1 as sf
left join segmentation.two_km as seg
	on sf.hectokey = seg.hectokey
group by hectokey_merged, end_datetime;




-------------------- 1 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.fifteenmin_one_km_v1;
create table features_speedflow_agg.fifteenmin_one_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg decimal,
	speed_diffxlanes_maxhecto_maxseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_avgseg decimal,
	speed_avgxlanes_diff_from_loop_behind_avgseg decimal,
	speed_avgxlanes_diff_from_loop_ahead_maxseg decimal,
	speed_avgxlanes_diff_from_loop_behind_maxseg decimal,
	speed_avgxlanes_diff_from_prev_rec_avgseg decimal,
	speed_avgxlanes_diff_from_prev_rec_maxseg decimal,
	flow_avgxlanes_avgseg decimal,
	flow_avgxlanes_maxseg decimal,
	flow_avgxlanes_minseg decimal,
	speed_avgxlanes_avgseg decimal,
	speed_avgxlanes_maxseg decimal,
	speed_avgxlanes_minseg decimal,
	speed_std_avgxlanes_maxseg decimal
);


insert into features_speedflow_agg.fifteenmin_one_km_v1
select 
	sf.end_datetime,
	seg.hectokey_merged,
	avg(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_avgseg,
	max(max_speed_diff_across_lanes) AS speed_diffxlanes_maxhecto_maxseg,
	avg(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	avg(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(avg_speed_diff_from_neighbor_loop_ahead) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(avg_speed_diff_from_neighbor_loop_behind) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	avg(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(avg_speed_diff_from_prev_recording) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	avg(avg_flow_on_hecto) AS flow_avgxlanes_avgseg,
	max(avg_flow_on_hecto) AS flow_avgxlanes_maxseg,
	min(avg_flow_on_hecto) AS flow_avgxlanes_minseg,
	avg(avg_speed_on_hecto) AS speed_avgxlanes_avgseg,
	max(avg_speed_on_hecto) AS speed_avgxlanes_maxseg,
	min(avg_speed_on_hecto) AS speed_avgxlanes_minseg,
	max(avg_speed_std_across_lanes) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_hecto_v1 as sf
left join segmentation.one_km as seg
	on sf.hectokey = seg.hectokey
group by hectokey_merged, end_datetime;
