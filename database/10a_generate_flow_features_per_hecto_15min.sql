--- Generates flow features per hectopunt (aggregates across lanes)

--- Schema: features_speedflow_agg

--- Tables
---		The featuers per hecto table is created (see Python script for name of hecto table).  



create schema if not exists $features_schema;

drop table if EXISTS $features_schema.$features_hecto_table;
create table $features_schema.$features_hecto_table(
	end_datetime timestamp without time zone,
	hectokey varchar,
	speed_avgxlanes decimal,
	speed_maxxlanes decimal,
	speed_minxlanes decimal,
	flow_totalxlanes decimal,
	flow_avgxlanes decimal,
	flow_maxxlanes decimal,
	flow_minxlanes decimal,
	speedstd_avgxlanes decimal,
	speedstd_maxxlanes decimal,
	flowstd_avgxlanes decimal,
	flowstd_maxxlanes decimal
);


insert into $features_schema.$features_hecto_table
select 
	end_datetime,
	hectokey,
	avg(speed_avg_all) as speed_avgxlanes,
	max(speed_avg_all) AS speed_maxxlanes,
	min(speed_avg_all) AS speed_minxlanes,
	sum(flow_avg_all) AS flow_totalxlanes,
	avg(flow_avg_all) AS flow_avgxlanes,
	max(flow_avg_all) AS flow_maxxlanes,
	min(flow_avg_all) AS flow_minxlanes,
	avg(speed_std_all) AS speedstd_avgxlanes,
	max(speed_std_all) AS speedstd_maxxlanes,
	avg(flow_std_all) AS flowstd_avgxlanes,
	max(flow_std_all) AS flowstd_maxxlanes
from $speedflow_loc
group by hectokey, end_datetime;

-- create additional features
alter TABLE $features_schema.$features_hecto_table
ADD COLUMN speed_maxdiffxlanes decimal,
ADD COLUMN speed_avgxlanes_diffloopahead decimal,
ADD COLUMN speed_avgxlanes_diffloopbehind decimal,
ADD COLUMN speed_avgxlanes_diffprevrec decimal,
ADD COLUMN flow_maxdiffxlanes decimal,
ADD COLUMN flow_totalxlanes_diffloopahead decimal,
ADD COLUMN flow_totalxlanes_diffloopbehind decimal,
ADD COLUMN flow_totalxlanes_diffprevrec decimal;


UPDATE  $features_schema.$features_hecto_table AS curr
SET speed_avgxlanes_diffprevrec = curr.speed_avgxlanes - prev.speed_avgxlanes,
flow_totalxlanes_diffprevrec = curr.flow_totalxlanes - prev.flow_totalxlanes
FROM $features_schema.$features_hecto_table AS prev
WHERE curr.hectokey = prev.hectokey
and curr.end_datetime = prev.end_datetime + interval '15 minute';

UPDATE $features_schema.$features_hecto_table
SET speed_maxdiffxlanes = speed_maxxlanes - speed_minxlanes,
flow_maxdiffxlanes = flow_maxxlanes - flow_minxlanes;


UPDATE $features_schema.$features_hecto_table curr
SET    speed_avgxlanes_diffloopahead = curr.speed_avgxlanes - adj.speed_avgxlanes,
flow_totalxlanes_diffloopahead = curr.flow_totalxlanes - adj.flow_totalxlanes
FROM   $features_schema.$features_hecto_table  adj
left join $networklink_loc link 
ON link.hectokey_ahead = adj.hectokey
WHERE  curr.end_datetime = adj.end_datetime
and link.hectokey = curr.hectokey;

UPDATE $features_schema.$features_hecto_table curr
SET    speed_avgxlanes_diffloopbehind = curr.speed_avgxlanes - adj.speed_avgxlanes,
flow_totalxlanes_diffloopbehind = curr.flow_totalxlanes - adj.flow_totalxlanes
FROM   $features_schema.$features_hecto_table  adj
left join $networklink_loc link 
ON link.hectokey_behind = adj.hectokey
WHERE  curr.end_datetime = adj.end_datetime
and link.hectokey = curr.hectokey;
