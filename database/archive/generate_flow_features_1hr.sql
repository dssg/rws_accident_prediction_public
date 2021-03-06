-- generate the flow features table using the data in speedflow

-- first do a group by on hecto
create schema if not exists features_speedflow_agg;

drop table if EXISTS features_speedflow_agg.onehour_hecto_v2;
create table features_speedflow_agg.onehour_hecto_v2(
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


insert into features_speedflow_agg.onehour_hecto_v2
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
from rws_clean.speedflow_1hr
group by hectokey, end_datetime;

-- create additional features
alter TABLE features_speedflow_agg.onehour_hecto_v2
add column max_speed_diff_across_lanes decimal,
ADD COLUMN avg_speed_diff_from_neighbor_loop_ahead DECIMAL,
ADD COLUMN avg_speed_diff_from_neighbor_loop_behind decimal,
ADD COLUMN avg_speed_diff_from_prev_recording decimal;


UPDATE  features_speedflow_agg.onehour_hecto_v2 AS curr
SET avg_speed_diff_from_prev_recording = curr.avg_speed_on_hecto - prev.avg_speed_on_hecto
FROM features_speedflow_agg.onehour_hecto_v2 AS prev
WHERE curr.hectokey = prev.hectokey
and curr.end_datetime = prev.end_datetime + interval '1 hour';

UPDATE features_speedflow_agg.onehour_hecto_v2
SET max_speed_diff_across_lanes = max_speed_across_lanes - min_speed_across_lanes;


UPDATE features_speedflow_agg.onehour_hecto_v2 curr
SET    avg_speed_diff_from_neighbor_loop_ahead = curr.avg_speed_on_hecto - adj.avg_speed_on_hecto
FROM   features_speedflow_agg.onehour_hecto_v2  adj
left join   rws_clean.flow_network link 
ON link.hectokey_ahead = adj.hectokey
WHERE  curr.end_datetime = adj.end_datetime
and link.hectokey = curr.hectokey;

UPDATE features_speedflow_agg.onehour_hecto_v2 curr
SET    avg_speed_diff_from_neighbor_loop_behind = curr.avg_speed_on_hecto - adj.avg_speed_on_hecto
FROM   features_speedflow_agg.onehour_hecto_v2  adj
left join   rws_clean.flow_network link 
ON link.hectokey_behind = adj.hectokey
WHERE  curr.end_datetime = adj.end_datetime
and link.hectokey = curr.hectokey;

-- Now aggregate across segments
-- 1 km aggregation
drop TABLE IF exists features_speedflow_agg.onehour_ten_km_v2;
create table features_speedflow_agg.onehour_ten_km_v2(
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


insert into features_speedflow_agg.onehour_ten_km_v2
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
from features_speedflow_agg.onehour_hecto_v2 as sf
left join segmentation.ten_km as seg
	on sf.hectokey = seg.hectokey
group by hectokey_merged, end_datetime;




