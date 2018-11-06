--- Generates flow features per segmentation level (aggregates across hectopunten)

--- Schema: features_speedflow_agg

--- Tables
---		A single table is created per segmentation scheme for features aggregated across the segment over the last 15 min. 
--		(See Python script to generate flow features for further details on naming scheme)


drop TABLE IF exists $features_schema.$features_segment_table;
create table $features_schema.$features_segment_table(
	end_datetime timestamp without time zone,
	hectokey_merged varchar,
	speed_maxdiffxlanes_avgxseg_15min_num decimal,
	speed_maxdiffxlanes_maxxseg_15min_num decimal,
	--- The feature below looks at the difference in speed_avgxlanes from the current loop to the loop that is ahead of it in terms of traffic flow
	speed_avgxlanes_diffloopahead_avgxseg_15min_num decimal,
	speed_avgxlanes_diffloopbehind_avgxseg_15min_num decimal,
	speed_avgxlanes_diffloopahead_maxxseg_15min_num decimal,
	speed_avgxlanes_diffloopbehind_maxxseg_15min_num decimal,
	--- The feature below looks at the difference in speed_avgxlanes in the current loop between the current time, and the value for the previous 15 minutes
	speed_avgxlanes_diffprevrec_avgxseg_15min_num decimal,
	speed_avgxlanes_diffprevrec_maxxseg_15min_num decimal,
	speed_avgxlanes_avgxseg_15min_num decimal,
	speed_avgxlanes_maxxseg_15min_num decimal,
	speed_avgxlanes_minxseg_15min_num decimal, -- use this feature to get speed_avgxlanes_maxdiffxseg_15min_num
	speed_minxlanes_minxseg_15min_num decimal,
	speed_minxlanes_avgxseg_15min_num decimal,
	flow_maxdiffxlanes_avgxseg_15min_num decimal,
	flow_maxdiffxlanes_maxxseg_15min_num decimal,
	flow_totalxlanes_diffloopahead_avgxseg_15min_num decimal,
	flow_totalxlanes_diffloopbehind_avgxseg_15min_num decimal,
	flow_totalxlanes_diffloopahead_maxxseg_15min_num decimal,
	flow_totalxlanes_diffloopbehind_maxxseg_15min_num decimal,
	flow_totalxlanes_diffprevrec_avgxseg_15min_num decimal,
	flow_totalxlanes_diffprevrec_maxxseg_15min_num decimal,
	flow_avgxlanes_avgxseg_15min_num decimal,
	flow_avgxlanes_maxxseg_15min_num decimal,
	flow_avgxlanes_minxseg_15min_num decimal, -- use this feature to get flow_totalxlanes_maxdiffxseg_15min_num
	flow_totalxlanes_totalxseg_15min_num decimal,
	speedstd_avgxlanes_avgxseg_15min_num decimal,
	speedstd_avgxlanes_maxxseg_15min_num decimal,
	speedstd_maxxlanes_maxxseg_15min_num decimal,
	flowstd_avgxlanes_avgxseg_15min_num decimal,
	flowstd_avgxlanes_maxxseg_15min_num decimal,
	flowstd_maxxlanes_maxxseg_15min_num decimal
);


insert into $features_schema.$features_segment_table
select 
	sf.end_datetime,
	seg.hectokey_merged,
	avg(speed_maxdiffxlanes) AS speed_maxdiffxlanes_avgxseg_15min_num,
	max(speed_maxdiffxlanes) AS speed_maxdiffxlanes_maxxseg_15min_num,
	avg(speed_avgxlanes_diffloopahead) AS speed_avgxlanes_diffloopahead_avgxseg_15min_num,
	avg(speed_avgxlanes_diffloopbehind) AS speed_avgxlanes_diffloopbehind_avgxseg_15min_num,
	max(speed_avgxlanes_diffloopahead) AS speed_avgxlanes_diffloopahead_maxxseg_15min_num,
	max(speed_avgxlanes_diffloopbehind) AS speed_avgxlanes_diffloopbehind_maxxseg_15min_num,
	avg(speed_avgxlanes_diffprevrec) AS speed_avgxlanes_diffprevrec_avgxseg_15min_num,
	max(speed_avgxlanes_diffprevrec) AS speed_avgxlanes_diffprevrec_maxxseg_15min_num,
	avg(speed_avgxlanes) AS speed_avgxlanes_avgxseg_15min_num,
	max(speed_avgxlanes) AS speed_avgxlanes_maxxseg_15min_num,
	min(speed_avgxlanes) AS speed_avgxlanes_minxseg_15min_num,
	min(speed_minxlanes) AS speed_minxlanes_minxseg_15min_num,
	avg(speed_minxlanes) AS speed_minxlanes_avgxseg_15min_num,
	avg(flow_maxdiffxlanes) AS flow_maxdiffxlanes_avgxseg_15min_num,
	max(flow_maxdiffxlanes) as flow_maxdiffxlanes_maxxseg_15min_num,
	avg(flow_totalxlanes_diffloopahead) AS flow_totalxlanes_diffloopahead_avgxseg_15min_num,
	avg(flow_totalxlanes_diffloopbehind) AS flow_totalxlanes_diffloopbehind_avgxseg_15min_num,
	max(flow_totalxlanes_diffloopahead) AS flow_totalxlanes_diffloopahead_maxxseg_15min_num,
	max(flow_totalxlanes_diffloopbehind) AS flow_totalxlanes_diffloopbehind_maxxseg_15min_num,
	avg(flow_totalxlanes_diffprevrec) AS flow_totalxlanes_diffprevrec_avgxseg_15min_num,
	max(flow_totalxlanes_diffprevrec) AS flow_totalxlanes_diffprevrec_maxxseg_15min_num,
	avg(flow_avgxlanes) AS flow_avgxlanes_avgxseg_15min_num,
	max(flow_avgxlanes) AS flow_avgxlanes_maxxseg_15min_num,
	min(flow_avgxlanes) AS flow_avgxlanes_minxseg_15min_num,
	sum(flow_totalxlanes) AS flow_totalxlanes_totalxseg_15min_num,
	avg(speedstd_avgxlanes) AS speedstd_avgxlanes_avgxseg_15min_num,
	max(speedstd_avgxlanes) AS speedstd_avgxlanes_maxxseg_15min_num,
	max(speedstd_maxxlanes) AS speedstd_maxxlanes_maxxseg_15min_num,
	avg(flowstd_avgxlanes) AS flowstd_avgxlanes_avgxseg_15min_num,
	max(flowstd_avgxlanes) AS flowstd_avgxlanes_maxxseg_15min_num,
	max(flowstd_maxxlanes) AS flowstd_maxxlanes_maxxseg_15min_num
from $features_schema.$features_hecto_table as sf
left join $segmentation_schema.$segmentation_table as seg
	on sf.hectokey = seg.hectokey
group by hectokey_merged, end_datetime;

-- index the data to improve performance
CREATE INDEX ${features_schema}_${features_segment_table}_hectokey_merged_idx ON $features_schema.$features_segment_table (hectokey_merged,end_datetime) ;


--update to add features related to difference across the segment
ALTER table $features_schema.$features_segment_table
ADD COLUMN  speed_avgxlanes_maxdiffxseg_15min_num decimal,
ADD COLUMN flow_totalxlanes_maxdiffxseg_15min_num decimal;


UPDATE $features_schema.$features_segment_table
SET speed_avgxlanes_maxdiffxseg_15min_num = speed_avgxlanes_maxxseg_15min_num - speed_avgxlanes_minxseg_15min_num,
flow_totalxlanes_maxdiffxseg_15min_num = flow_avgxlanes_maxxseg_15min_num - flow_avgxlanes_minxseg_15min_num;
