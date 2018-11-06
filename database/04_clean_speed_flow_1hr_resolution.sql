--- Clean the flow and speed data.  Speed and flow were provided as two separate cvs's.  This code joins the two datasets
--- and preprocesses the speed and flow data
--- Schema: Rws_Clean

--- Tables
--- 	speedflow_1hr: flow and speed (not always available) for loops in the Utrecht region as averaged over each hour. 
---			Breakdowns of flow and speed per vehicle weight category are included as separate columns.



drop table if exists rws_clean.flow_1hr;
drop table if exists rws_clean.speed_1hr;

--create duplicate of both speed and flow in the clean dataset
CREATE TABLE rws_clean.flow_1hr AS SELECT 
"measurementSiteReference",
"periodEnd",
"standardDeviation",
"dataError",
"avgVehicleFlow",
"avgVehicleSpeed",
"specificLane",
"specificVehicleCharacteristics",
"startLocatieForDisplayLat",
"startLocatieForDisplayLong",
"alertCDirectionCoded",
"ROADNUMBER",
"carriageway"
FROM rws_raw.flow_1hr_resolution;


CREATE TABLE rws_clean.speed_1hr AS SELECT 
"measurementSiteReference",
"periodEnd",
"standardDeviation",
"dataError",
"avgVehicleSpeed",
"specificLane",
"specificVehicleCharacteristics",
"startLocatieForDisplayLat",
"startLocatieForDisplayLong",
"alertCDirectionCoded",
"ROADNUMBER",
"carriageway"
FROM rws_raw.speed_1hr_resolution;
	
-- create a unique id in the flow and speed table for each loop at a certain time. 
-- Use hash function to generate unique id
alter table rws_clean.flow_1hr add COLUMN "loop_time_id" varchar;
alter table rws_clean.speed_1hr add column "loop_time_id" varchar;

update rws_clean.flow_1hr
set loop_time_id = md5(concat("measurementSiteReference",'.',"periodEnd",'.',"specificLane",'.',
	carriageway,'.',"specificVehicleCharacteristics",'.'));

update rws_clean.speed_1hr
set loop_time_id = md5(concat("measurementSiteReference",'.',"periodEnd",'.',"specificLane",'.',
	carriageway,'.',"specificVehicleCharacteristics",'.'));

-- after doing this, create index on loop_time_id in both speed and flow dataset
CREATE INDEX speed_loop_time_id_idx ON rws_clean.speed_1hr (loop_time_id) ;
CREATE INDEX flow_loop_time_id_idx ON rws_clean.flow_1hr (loop_time_id) ;


-- add column for speed std
alter table rws_clean.flow_1hr
add column speed_std_raw decimal;


-- delete rows where  data error is 1
delete from rws_clean.speed_1hr
where "dataError" = '1';

delete from rws_clean.flow_1hr
where "dataError" = '1';

-- insert the speed column into flow table, along with the standard deviation and computation method
update rws_clean.flow_1hr as f 
set "avgVehicleSpeed" = s."avgVehicleSpeed",
speed_std_raw = s."standardDeviation"
from rws_clean.speed_1hr as s
where s.loop_time_id = f.loop_time_id;

-- now drop the speed table
drop table rws_clean.speed_1hr;

-- add new columns needed for different vehicle weights
alter table rws_clean.flow_1hr
add column road_num varchar,
add column road_dir varchar,
add column flow_avg_all decimal,
add column flow_std_all decimal,
add column flow_avg_w1 decimal, -- smallest weight category
add column flow_std_w1 decimal,	
add column flow_avg_w2 decimal, -- middle weight category
add column flow_std_w2 decimal,
add column flow_avg_w3 decimal, -- largest weight category
add column flow_std_w3 decimal,
add column speed_avg_all decimal,
add column speed_std_all decimal,
add column speed_avg_w1 decimal,
add column speed_std_w1 decimal,
add column speed_avg_w2 decimal,
add column speed_std_w2 decimal,
add column speed_avg_w3 decimal,
add column speed_std_w3 decimal;

-- now insert appropriate values for each new weight category
update rws_clean.flow_1hr
set flow_avg_all = case when "specificVehicleCharacteristics" = 'anyVehicle' 
		   then "avgVehicleFlow"
		   end,
	flow_std_all =
 		case when "specificVehicleCharacteristics" = 'anyVehicle'
           	then "standardDeviation" 
        	end,
	flow_avg_w1 =
		case when "specificVehicleCharacteristics" in ('greaterThan 1.85 and lessThanOrEqualTo 2.40',
							       'greaterThan 2.40 and lessThanOrEqualTo 5.60','lessThanOrEqualTo 5.60')
           	then "avgVehicleFlow"            
       		end,
	flow_std_w1 = 
		case when "specificVehicleCharacteristics" in ('greaterThan 1.85 and lessThanOrEqualTo 2.40',
							       'greaterThan 2.40 and lessThanOrEqualTo 5.60',
							       'lessThanOrEqualTo 5.60')
           	then "standardDeviation"            
        	end,
	flow_avg_w2 =
		case when "specificVehicleCharacteristics" in ('greaterThan 5.60 and lessThanOrEqualTo 11.50',
							       'greaterThan 11.50 and lessThanOrEqualTo 12.20',
							       'greaterThan 5.60 and lessThanOrEqualTo 12.20')
           	then "avgVehicleFlow"           
        	end,
	flow_std_w2 =
		case when "specificVehicleCharacteristics" in ('greaterThan 5.60 and lessThanOrEqualTo 11.50',
							       'greaterThan 11.50 and lessThanOrEqualTo 12.20',
							       'greaterThan 5.60 and lessThanOrEqualTo 12.20')
           	then "standardDeviation"           
        	end,
	flow_avg_w3 =
		case when "specificVehicleCharacteristics" = 'greaterThan 12.20'
           	then "avgVehicleFlow"
        	end,
	flow_std_w3 =
		case when "specificVehicleCharacteristics" = 'greaterThan 12.20'
           	then "standardDeviation" 
        	end;
	
          
update rws_clean.flow_1hr
set speed_avg_all = case when "specificVehicleCharacteristics" = 'anyVehicle'
		   then "avgVehicleSpeed"
		   end,
    speed_std_all =
 		case when "specificVehicleCharacteristics" = 'anyVehicle'
           	then "speed_std_raw" 
        	end,
	speed_avg_w1 =
		case when "specificVehicleCharacteristics" in ('greaterThan 1.85 and lessThanOrEqualTo 2.40',
							       'greaterThan 2.40 and lessThanOrEqualTo 5.60',
							       'lessThanOrEqualTo 5.60')
           	then "avgVehicleSpeed"            
       		end,
	speed_std_w1 = 
		case when "specificVehicleCharacteristics" in ('greaterThan 1.85 and lessThanOrEqualTo 2.40',
							       'greaterThan 2.40 and lessThanOrEqualTo 5.60','lessThanOrEqualTo 5.60')
           	then "speed_std_raw"            
        	end,
	speed_avg_w2 =
		case when "specificVehicleCharacteristics" in ('greaterThan 5.60 and lessThanOrEqualTo 11.50',
							       'greaterThan 11.50 and lessThanOrEqualTo 12.20',
							       'greaterThan 5.60 and lessThanOrEqualTo 12.20')
           	then "avgVehicleSpeed"           
        	end,
	speed_std_w2 =
		case when "specificVehicleCharacteristics" in ('greaterThan 5.60 and lessThanOrEqualTo 11.50',
							       'greaterThan 11.50 and lessThanOrEqualTo 12.20',
							       'greaterThan 5.60 and lessThanOrEqualTo 12.20')
           	then "speed_std_raw"           
        	end,
	speed_avg_w3 =
		case when "specificVehicleCharacteristics" = 'greaterThan 12.20'
           	then "avgVehicleSpeed"
        	end,
	speed_std_w3 =
		case when "specificVehicleCharacteristics" = 'greaterThan 12.20'
           	then "speed_std_raw" 
        	end;

-- add road direction and road number	   
update rws_clean.flow_1hr as f
set road_num = substring(f."ROADNUMBER",2,2),
	road_dir = 
		case
			when "ROADNUMBER" = 'A2' then
				case 
					when "alertCDirectionCoded" = 'positive' then 'Li'
					else 'Re'
				end
			when "ROADNUMBER" = 'A2 hrb' then
				case 
					when "alertCDirectionCoded" = 'positive' then 'Li'
					else 'Re'
				end	
		else 
			case 
				when "alertCDirectionCoded" = 'positive' then 'Re'
				else 'Li'
			end
		end;
	
-- now create a new table that is a group by (over location and time, to combine vehicleChars) of current table.  
-- then delete the temporary clean table
drop table if exists rws_clean.speedflow_1hr;

CREATE TABLE rws_clean.speedflow_1hr(
	loop_id varchar not null,
	end_datetime timestamp without time zone not null,
	lat decimal not null,
	long decimal not null,
	flow_avg_all decimal,
	flow_std_all decimal,
	flow_avg_w1 decimal,
	flow_std_w1 decimal,
	flow_avg_w2 decimal,
	flow_std_w2 decimal,
	flow_avg_w3 decimal,
	flow_std_w3 decimal,
	speed_avg_all decimal,
	speed_std_all decimal,
	speed_avg_w1 decimal,
	speed_std_w1 decimal,
	speed_avg_w2 decimal,
	speed_std_w2 decimal,
	speed_avg_w3 decimal,
	speed_std_w3 decimal,
	road_lane varchar,
	road_num decimal not null,
	road_dir varchar,
	road_type varchar	
);
	
-- do a group by and pull in the relevant columns of the data
-- used to consolidate over all the vehicleChar in order to create single obs

insert into rws_clean.speedflow_1hr
select 
	"measurementSiteReference" as loop_id,
	"periodEnd" as end_datetime,
	max(cast("startLocatieForDisplayLat" as decimal)) as lat,
	max(cast("startLocatieForDisplayLong" as decimal)) as long,
	max(flow_avg_all),
	max(flow_std_all),
	max(flow_avg_w1),
	max(flow_std_w1),           
	max(flow_avg_w2),
	max(flow_std_w2),
	max(flow_avg_w3), 
	max(flow_std_w3),
	max(speed_avg_all),
	max(speed_std_all),
	max(speed_avg_w1),
	max(speed_std_w1),           
	max(speed_avg_w2),
	max(speed_std_w2),
	max(speed_avg_w3), 
	max(speed_std_w3),
	"specificLane" as road_lane,
	cast(road_num as decimal),
	road_dir,
	carriageway as road_type
from rws_clean.flow_1hr
group by end_datetime, loop_id, road_num, "carriageway", road_dir, "specificLane";

--drop clean flow, they will still exist in the raw schema. 
--from now on use the grouped table
drop table rws_clean.flow_1hr;


-- add hectokey and loop_unique_key	
alter table rws_clean.speedflow_1hr 
add column "loop_unique_key" varchar, 
add column "hectokey" varchar,
add column "dvk_or_dir" varchar;

-- add road type, it will need to be updated when we use side roads
update rws_clean.speedflow_1hr
set dvk_or_dir = 
	case 
		when road_type = 'mainCarriageway' then road_dir
		else NULL
	  end;
		 
-- for each loop, find the corresponding hecto poitn using the flow_hect_link table
-- add unique loop id so can join with flow_hect_link table
update rws_clean.speedflow_1hr
set loop_unique_key = concat(loop_id,'.',road_lane,'.',dvk_or_dir,'.',road_dir);
	
-- make index on the loop_unique_key for both speedflow and flow_hect link to make join faster
CREATE INDEX if not exists flow_hect_link_loop_unique_key_idx ON rws_clean.flow_hect_link (loop_unique_key) ;
CREATE INDEX speedflow_1hr_loop_unique_key_idx ON rws_clean.speedflow_1hr (loop_unique_key) ;

-- link the hectokey for each loop
update rws_clean.speedflow_1hr as speedflow
set hectokey = link.hectokey
from rws_clean.flow_hect_link as link
where speedflow.loop_unique_key = link.loop_unique_key;

