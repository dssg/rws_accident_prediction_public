/* Creating ongevallen/hecopunten link
 * ongekey is unique identifier in ongevallen
 * hectoyearkey is unique identifier in hectopunten*/
create table rws_clean.ong_hect_link(
	ongekey varchar not null,
	hectoyearkey varchar not null
);

--selecting connection btwn onge and hecto for the same hectopoint
--where the accident is more recent than the hecto observation, aka o.year - h.year >= 0 
--and choosing the hecto observation closest in time to the accident (by distinct on)
insert into rws_clean.ong_hect_link
select distinct on (o.ongekey) 
	o.ongekey, h.hectoyearkey
from rws_clean.ongevallen as o
left outer join rws_clean.hectopunten as h
	on h.hectokey = o.hectokey
where extract(year from o.datetime)::decimal - h.year >= 0 
order by ongekey, extract(year from o.datetime)::decimal - h.year;

--select * from rws_clean.ong_hect_link
