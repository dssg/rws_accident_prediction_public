with intervals as (
	select 
  		(select min(periodstart)::date from rws_schema.intensity) + ( n    || ' minutes')::interval as periodstart,
  		(select min(periodstart)::date from rws_schema.intensity) + ((n+60) || ' minutes')::interval as periodend
	from generate_series(0, ((select max(periodstart)::date - min(periodstart)::date from rws_schema.intensity) + 1)*24*60, 60) as n
	limit 20
)
select i.periodstart, i.periodend, n.measurementsitereference, avg(n.avgvehicleflow)
from rws_schema.intensity as n
right join intervals as i
on to_timestamp(n.periodstart, 'YYYY-MM-DD HH24:MI:SS') >= i.periodstart 
	and to_timestamp(n.periodend, 'YYYY-MM-DD HH24:MI:SS') < i.periodend
where n.periodstart between '2016-01-01' and '2016-01-05'
group by i.periodstart, i.periodend, n.measurementsitereference
limit 10;



with intervals as (
	select 
  		(select min(periodstart)::date from rws_schema.intensity) + ( n    || ' minutes')::interval as periodstart,
  		(select min(periodstart)::date from rws_schema.intensity) + ((n+60) || ' minutes')::interval as periodend
	from generate_series(0, ((select max(periodstart)::date - min(periodstart)::date from rws_schema.intensity) + 1)*24*60, 60) as n
),
n as (
	select * 
	from rws_schema.intensity
	where dataerror <> '1'
		and periodstart between '2016-01-01' and '2017-01-01' --edit as needed
		and specificvehiclecharacteristics = 'anyVehicle' --edit as needed
)
select i.periodstart, i.periodend, n.measurementsitereference, avg(n.avgvehicleflow) as avgflow
from n
join intervals as i
on to_timestamp(n.periodstart, 'YYYY-MM-DD HH24:MI:SS') >= i.periodstart 
	and to_timestamp(n.periodend, 'YYYY-MM-DD HH24:MI:SS') < i.periodend
group by i.periodstart, i.periodend, n.measurementsitereference
limit 10;