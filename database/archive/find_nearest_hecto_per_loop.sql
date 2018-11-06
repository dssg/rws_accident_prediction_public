-- NOTE: This file is not currently being used.  We are using the dataset provied by CSResearch, rws_raw.utrecht_rws_loops to get the closest hecto
-- This file may be useful when we need to query the nearest hecto for loops not on the main carriageway
select 
    "measurementSiteName1",
    "ROADNUMBER",
    "measurementSide",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "carriageway",
    "generatedSiteName"
from 
    rws_raw.utrecht_1hr 
limit 100; 

select *
from    
    rws_clean.hectopunten
where
    road_num = 1 and hecto = 44.5

select *
from
    rws_clean.ongevallen


------testing join mechanism
with flow as 
    (select
        distinct on (location, "alertCDirectionCoded") "measurementSiteName1" as location,
        "startLocatieForDisplayLong"::decimal as long,
        "startLocatieForDisplayLat"::decimal as lat,
        regexp_replace("ROADNUMBER", '[^0-9]', '')::decimal as road_num,
        case
            when ("alertCDirectionCoded" = 'positive') then 'Re'
            when ("alertCDirectionCoded" = 'negative') then 'Li'
            else null
        end as road_dir,
        "measurementSide" as side
    from
        rws_raw.utrecht_1hr
    where 
        carriageway = 'mainCarriageway'
        and "startLocatieForDisplayLat"::decimal > 50.5 
        and "startLocatieForDisplayLat"::decimal < 52.5 
        and "startLocatieForDisplayLong"::decimal > 4
        and "startLocatieForDisplayLong"::decimal < 6
        and "measurementSiteName1" in ('0021hrl0802ra')  
        --for test cases '0021hrl0802ra', '0121hrr0409ra', '0271hrr0678ra', '0271hrr0674ra'
    ),
hecto as 
    (select
        distinct on (hectokey) hectokey,
        lat,
        long,
        road_num,
        road_dir
    from 
        rws_clean.hectopunten
    where road_type is null
    )

select
    m.hectokey,
    m.hect_lat,
    m.hect_long,
    f.lat,
    f.long,
    f.location,
    m.dist
from
    flow as f
left join lateral 
    (select 
        hectokey,
        ST_Distance(ST_MakePoint(h.long, h.lat), ST_MakePoint(f.long, f.lat)) as dist, 
        f.lat,
        f.long,
        h.lat as hect_lat, 
        h.long as hect_long
    from 
        hecto as h
    where 
        f.road_num = h.road_num and f.road_dir = h.road_dir
    order by 
        ST_Distance(ST_MakePoint(h.long, h.lat),
        ST_MakePoint(f.long, f.lat)) asc limit 2) as m
    on true -- dist < 0.0009 --only join when dist is less than threshhold, else NULL
    order by dist asc;

select * from rws_clean.ongevallen limit 10;

select distinct("measurementSiteName1") 
from 
    rws_raw.utrecht_1hr 
where 
    "measurementSiteName1" like '012%' and carriageway = 'mainCarriageway' limit 10;
