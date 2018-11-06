-- Note this table is not being used for pipeline v1

-- create a table that links each hecto to both the loop ahead of it, and the loop behind it
-- loops are identified as the hectokey that they are closest to
drop table if exists rws_clean.hect_flow_link;

create table rws_clean.hect_flow_link(
	hectokey varchar,
	hectokey_loop_ahead varchar,
	loop_unique_key_ahead varchar,
	hectokey_loop_behind varchar,
	loop_unique_key_behind varchar
);
-- may need to change since multiple hectopoints for same road
-- do linkage just for Re direction roads
with linked as -- code to take the difference between each hectopoint and the hectopoints with loops
	(select 
		h.hectokey as hectokey, 
		f.hectokey as loop_hectokey, 
		h.hecto - f.hecto as hdiff_behind, --distance from hectopoint to hecto behind it  
		f.hecto - h.hecto as hdiff_ahead,  --distance from hectopoint to hecto in front of it
		h.road_num, 
		h.road_type, 
		h.road_dir,
		f.loop_unique_key
	from rws_clean.hectopunten as h
	inner join rws_clean.flow_hect_link as f
		on h.road_num = f.road_num 
			and h.road_dir = f.road_dir
	where h.road_type is null  -- change this and add to on statement when adding different types of highways
		and h.road_dir = 'Re'),
behind as -- * code to get loop behind
	(select distinct on (hectokey)
		hectokey, 
		loop_hectokey, 
		loop_unique_key
	from linked
	where hdiff_behind >=0 --hecto > flow_hecto
	order by linked.hectokey, hdiff_behind asc),
ahead as -- * code to get loop ahead
	(select distinct on (hectokey)
		hectokey, 
		loop_hectokey, 
		loop_unique_key
	from linked
	where hdiff_ahead >=0 --hecto < flow_hecto
	order by hectokey, hdiff_ahead asc)
insert into rws_clean.hect_flow_link
select 
	ahead.hectokey, 
	ahead.loop_hectokey as hectokey_loop_ahead,
	ahead.loop_unique_key as loop_unique_key_ahead,
	behind.loop_hectokey as hectokey_loop_behind,
	behind.loop_unique_key as loop_unique_key_behind
from ahead
full outer join behind
on behind.hectokey = ahead.hectokey;

-- do linkage just for Li direction roads
with linked as -- code to take the difference between each hectopoint and the hectopoints with loops
	(select 
		h.hectokey as hectokey, 
		f.hectokey as loop_hectokey, 
		f.hecto - h.hecto as hdiff_behind, --distance from hectopoint to hecto behind it
		h.hecto - f.hecto as hdiff_ahead,  --distance from hectopoint to hecto in front of it
		h.road_num, 
		h.road_type, 
		h.road_dir,
		f.loop_unique_key
	from rws_clean.hectopunten as h
	inner join rws_clean.flow_hect_link as f
		on h.road_num = f.road_num 
			and h.road_dir = f.road_dir
	where h.road_type is null  -- change this and add to on statement when adding different types of highways
		and h.road_dir = 'Li'),
behind as -- * code to get loop behind
	(select distinct on (hectokey)
		hectokey, 
		loop_hectokey, 
		loop_unique_key
	from linked
	where hdiff_behind >=0 --hecto < flow_hecto
	order by linked.hectokey, hdiff_behind asc),
ahead as -- * code to get loop ahead
	(select distinct on (hectokey)
		hectokey, 
		loop_hectokey, 
		loop_unique_key
	from linked
	where hdiff_ahead >=0 --hecto > flow_hecto
	order by hectokey, hdiff_ahead asc)
insert into rws_clean.hect_flow_link
select 
	ahead.hectokey, 
	ahead.loop_hectokey as hectokey_loop_ahead,
	ahead.loop_unique_key as loop_unique_key_ahead,
	behind.loop_hectokey as hectokey_loop_behind,
	behind.loop_unique_key as loop_unique_key_behind
from ahead
full outer join behind
on behind.hectokey = ahead.hectokey;
