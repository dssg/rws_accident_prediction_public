-- link each flow loop to both the loop behind it, and the loop in front of it in terms of traffic flow
-- note this is aggregated across loops in the same hectokey across lanes

drop table if exists rws_clean.flow_network;

create table rws_clean.flow_network(
loop_id varchar,
hectokey varchar,
loop_id_ahead varchar,
hectokey_ahead varchar,
loop_id_behind varchar,
hectokey_behind varchar
);


-- run for just Re roads
with grped_loops as (
	select loop_id,
		   hectokey,
		   hecto,
		   road_num,
		   road_type,
		   road_dir
	from rws_clean.flow_hect_link
	group by loop_id, hectokey, hecto, road_num, road_type,road_dir),
linked as -- code to take the difference between each loop hectopoint and the other loop hectopoints
	(select 
		h.loop_id,
		h.hectokey, 
		f.loop_id as other_loop, 
		f.hectokey as other_hectokey,
		h.hecto - f.hecto as distance_behind, --distance from loop to other loop behind it 
		f.hecto - h.hecto as distance_ahead,  --distance from loop to other loop in front of it
		h.road_num, 
		h.road_type, 
		h.road_dir
	from grped_loops as h
	inner join grped_loops as f
		on h.road_num = f.road_num 
			and h.road_dir = f.road_dir
	where h.road_type is null  -- change this and add to on statement when adding different types of highways
		and h.road_dir = 'Re'),
behind as -- * code to get other loop behind
	(select distinct on (loop_id)
		loop_id,
		hectokey,
		other_loop,
		other_hectokey
	from linked
	where distance_behind >0 --loop > other_loop
	order by loop_id, distance_behind asc),
ahead as -- * code to get loop ahead
	(select distinct on (loop_id)
		loop_id,
		hectokey,
		other_loop,
		other_hectokey
	from linked
	where distance_ahead > 0 --loop < other_loop
	order by loop_id, distance_ahead asc)
insert into rws_clean.flow_network
select 
	ahead.loop_id, 
	ahead.hectokey,
	ahead.other_loop as loop_id_ahead,
	ahead.other_hectokey as hectokey_ahead,
	behind.other_loop as loop_id_behind,
	behind.other_hectokey as hectokey_behind
from ahead
full outer join behind
on behind.loop_id = ahead.loop_id;





-- run for Li roads
with grped_loops as (
	select loop_id,
		   hectokey,
		   hecto,
		   road_num,
		   road_type,
		   road_dir
	from rws_clean.flow_hect_link
	group by loop_id, hectokey, hecto, road_num, road_type,road_dir),
linked as -- code to take the difference between each loop hectopoint and the other loop hectopoints
	(select 
		h.loop_id,
		h.hectokey, 
		f.loop_id as other_loop, 
		f.hectokey as other_hectokey,
		f.hecto - h.hecto as distance_behind, --distance from loop to other loop behind it 
		h.hecto - f.hecto as distance_ahead,  --distance from loop to other loop in front of it
		h.road_num, 
		h.road_type, 
		h.road_dir
	from grped_loops as h
	inner join grped_loops as f
		on h.road_num = f.road_num 
			and h.road_dir = f.road_dir
	where h.road_type is null  -- change this and add to on statement when adding different types of highways
		and h.road_dir = 'Li'),
behind as -- * code to get other loop behind
	(select distinct on (loop_id)
		loop_id,
		hectokey,
		other_loop,
		other_hectokey
	from linked
	where distance_behind >0 --loop > other_loop
	order by loop_id, distance_behind asc),
ahead as -- * code to get loop ahead
	(select distinct on (loop_id)
		loop_id,
		hectokey,
		other_loop,
		other_hectokey
	from linked
	where distance_ahead > 0 --loop < other_loop
	order by loop_id, distance_ahead asc)
insert into rws_clean.flow_network
select 
	ahead.loop_id, 
	ahead.hectokey,
	ahead.other_loop as loop_id_ahead,
	ahead.other_hectokey as hectokey_ahead,
	behind.other_loop as loop_id_behind,
	behind.other_hectokey as hectokey_behind
from ahead
full outer join behind
on behind.loop_id = ahead.loop_id;
