--- This script generated features that use the speed and flow data by aggregating from 15 min to 1 hour. 
--- The resulting table will be called by the Python pipeline script to use for prediction

--- Schema: features_speedflow_agg

--- Tables
--- 	onehour_aggfrom15min_ten_km_v1: Uses the fifteenmin_ten_km_v1 table to aggregate features over time

--- 	onehour_aggfrom15min_five_km_v1: aggregates for one hour from fifteenmin_five_km_v1

--- 	onehour_aggfrom15min_one_km_v1: aggregates for one hour from fifteenmin_one_km_v1


-- generate the flow features table using the data in speedflow

-- first do a group by on hecto

-------------------- 10 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.onehour_aggfrom15min_ten_km_v1;
create table features_speedflow_agg.onehour_aggfrom15min_ten_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg_avg15min decimal,
	speed_diffxlanes_maxhecto_maxseg_avg15min  decimal,
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


insert into features_speedflow_agg.onehour_aggfrom15min_ten_km_v1
select 
	date_trunc('hour', end_datetime + interval '1 hour') AS end_datetime,
	hectokey_merged,
	max(speed_diffxlanes_maxhecto_avgseg) AS speed_diffxlanes_maxhecto_avgseg,
	max(speed_diffxlanes_maxhecto_maxseg) AS speed_diffxlanes_maxhecto_maxseg,
	max(speed_avgxlanes_diff_from_loop_ahead_avgseg) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	max(speed_avgxlanes_diff_from_loop_behind_avgseg) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(speed_avgxlanes_diff_from_loop_ahead_maxseg) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(speed_avgxlanes_diff_from_loop_behind_maxseg) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	max(speed_avgxlanes_diff_from_prev_rec_avgseg) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(speed_avgxlanes_diff_from_prev_rec_maxseg) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	max(flow_avgxlanes_avgseg) AS flow_avgxlanes_avgseg,
	max(flow_avgxlanes_maxseg) AS flow_avgxlanes_maxseg,
	min(flow_avgxlanes_minseg) AS flow_avgxlanes_minseg,
	max(speed_avgxlanes_avgseg) AS speed_avgxlanes_avgseg,
	max(speed_avgxlanes_maxseg) AS speed_avgxlanes_maxseg,
	min(speed_avgxlanes_minseg) AS speed_avgxlanes_minseg,
	max(speed_std_avgxlanes_maxseg) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_ten_km_v1
GROUP BY end_datetime, hectokey_merged;

-------------------- 5 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.onehour_aggfrom15min_five_km_v1;
create table features_speedflow_agg.onehour_aggfrom15min_five_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg_avg15min decimal,
	speed_diffxlanes_maxhecto_maxseg_avg15min  decimal,
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


insert into features_speedflow_agg.onehour_aggfrom15min_five_km_v1
select 
	date_trunc('hour', end_datetime + interval '1 hour') AS end_datetime,
	hectokey_merged,
	max(speed_diffxlanes_maxhecto_avgseg) AS speed_diffxlanes_maxhecto_avgseg,
	max(speed_diffxlanes_maxhecto_maxseg) AS speed_diffxlanes_maxhecto_maxseg,
	max(speed_avgxlanes_diff_from_loop_ahead_avgseg) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	max(speed_avgxlanes_diff_from_loop_behind_avgseg) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(speed_avgxlanes_diff_from_loop_ahead_maxseg) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(speed_avgxlanes_diff_from_loop_behind_maxseg) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	max(speed_avgxlanes_diff_from_prev_rec_avgseg) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(speed_avgxlanes_diff_from_prev_rec_maxseg) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	max(flow_avgxlanes_avgseg) AS flow_avgxlanes_avgseg,
	max(flow_avgxlanes_maxseg) AS flow_avgxlanes_maxseg,
	min(flow_avgxlanes_minseg) AS flow_avgxlanes_minseg,
	max(speed_avgxlanes_avgseg) AS speed_avgxlanes_avgseg,
	max(speed_avgxlanes_maxseg) AS speed_avgxlanes_maxseg,
	min(speed_avgxlanes_minseg) AS speed_avgxlanes_minseg,
	max(speed_std_avgxlanes_maxseg) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_five_km_v1
GROUP BY end_datetime, hectokey_merged;

-------------------- 1 km aggregation --------------------
drop TABLE IF exists features_speedflow_agg.onehour_aggfrom15min_one_km_v1;
create table features_speedflow_agg.onehour_aggfrom15min_one_km_v1(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_diffxlanes_maxhecto_avgseg_avg15min decimal,
	speed_diffxlanes_maxhecto_maxseg_avg15min  decimal,
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


insert into features_speedflow_agg.onehour_aggfrom15min_one_km_v1
select 
	date_trunc('hour', end_datetime + interval '1 hour') AS end_datetime,
	hectokey_merged,
	max(speed_diffxlanes_maxhecto_avgseg) AS speed_diffxlanes_maxhecto_avgseg,
	max(speed_diffxlanes_maxhecto_maxseg) AS speed_diffxlanes_maxhecto_maxseg,
	max(speed_avgxlanes_diff_from_loop_ahead_avgseg) AS speed_avgxlanes_diff_from_loop_ahead_avgseg,
	max(speed_avgxlanes_diff_from_loop_behind_avgseg) AS speed_avgxlanes_diff_from_loop_behind_avgseg,
	max(speed_avgxlanes_diff_from_loop_ahead_maxseg) AS speed_avgxlanes_diff_from_loop_ahead_maxseg,
	max(speed_avgxlanes_diff_from_loop_behind_maxseg) AS speed_avgxlanes_diff_from_loop_behind_maxseg,
	max(speed_avgxlanes_diff_from_prev_rec_avgseg) AS speed_avgxlanes_diff_from_prev_rec_avgseg,
	max(speed_avgxlanes_diff_from_prev_rec_maxseg) AS speed_avgxlanes_diff_from_prev_rec_maxseg,
	max(flow_avgxlanes_avgseg) AS flow_avgxlanes_avgseg,
	max(flow_avgxlanes_maxseg) AS flow_avgxlanes_maxseg,
	min(flow_avgxlanes_minseg) AS flow_avgxlanes_minseg,
	max(speed_avgxlanes_avgseg) AS speed_avgxlanes_avgseg,
	max(speed_avgxlanes_maxseg) AS speed_avgxlanes_maxseg,
	min(speed_avgxlanes_minseg) AS speed_avgxlanes_minseg,
	max(speed_std_avgxlanes_maxseg) AS speed_std_avgxlanes_maxseg
from features_speedflow_agg.fifteenmin_one_km_v1
GROUP BY end_datetime, hectokey_merged;

