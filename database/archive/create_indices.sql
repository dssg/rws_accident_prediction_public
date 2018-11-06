create index on rws_schema.hectopunten(key);
create index on rws_schema.hectopunten(jaar);
create index on rws_schema.hectopunten(id_jaar);
create index on rws_schema.hectopunten(id);

create index on rws_schema.ongevallen(key);
create index on rws_schema.ongevallen(weg);

create index on rws_schema.intensity(periodstart, periodend DESC);

create index on rws_raw.speed("measurementSiteReference");

create index on rws_raw.flow("measurementSiteReference", "periodStart");