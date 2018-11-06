-- This script links each loop to the nearest hectopunten, using a datset provided by CSResearch, rws_raw.utrecht_rws_loops

-- add columns to dataset provided by CS Research for road direction and dvk_letter
alter table rws_raw.utrecht_rws_loops 
add column "road_dir" varchar,
add column "dvk_or_dir" varchar,
add column "road_num" decimal;


-- add road dir to utrecht data
update rws_raw.utrecht_rws_loops
set road_dir = 
case when "highway" = 'A2' then
	case when "dir" = 'positive' then 'Li'
		else 'Re'
		end
	when "highway" = 'A2 hrb' then
		case when "dir" = 'positive' then 'Li'
		else 'Re'
		end	
	else 
		case when "dir" = 'positive' then 'Re'
		else 'Li'
		end
	end;

-- set variable that is set to Li/Re when main carriageway, and dvk letter if not
-- This will need to be updated when we have roads besides main carriageway
update rws_raw.utrecht_rws_loops
set dvk_or_dir = 
(case 
	when "desc_en" = 'mainCarriageway' then road_dir
	else NULL
end);
	
update rws_raw.utrecht_rws_loops
set road_num = cast(substring(highway,2,2) as decimal);
	
-- Create table to link each loop to a hectokey.  
-- Give each loop a unique identifier (measurementSiteReference is not unique across lanes)
drop table if exists rws_clean.flow_hect_link;
create table rws_clean.flow_hect_link(
	loop_unique_key varchar not null,
	loop_id varchar,
	hectokey varchar not null,
	road_lane varchar,
	road_num decimal,
	road_type varchar,
	hecto decimal,
	road_dir varchar,
	lat decimal,
	long decimal
);


insert into rws_clean.flow_hect_link
select
	-- add logic if we use loops beside MONIBAS
	concat(loop_id,'.',lane,'.',dvk_or_dir,'.',road_dir) as loop_unique_key,
	loop_id,
	concat(road_num,'.',"proximal_hecto",'.',dvk_or_dir, '.',road_dir) as hectokey,
	lane AS road_lane,
	road_num,
	--note for our data now we only have main carriageway, this will need to be updated if we use other roads
	case 
		when u.desc_en = 'mainCarriageway' then null 
		else 'TBD' 
	end as road_type,
	u.proximal_hecto*.1 as hecto,
	u.road_dir as road_dir,
	lat,
	lon as long
from rws_raw.utrecht_rws_loops as u;


-- check there are no loop ids with more than one hectopunten
--select count(distinct(hectokey))
--from rws_clean.flow_hect_link
--group by loop_unique_key
--having count(distinct(hectokey)) > 1;
