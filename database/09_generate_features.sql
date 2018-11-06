--- This script generates the schema that will be used to store feature tables that do not include speed and flow
--- May be updated in the future to nclude other feature generation code that is easier to implement in SQL than in Python
--- Update: now includes feature generation for some static features of hectopunten based off 2012-2016 data

--- Schema: features_hectopunten_agg, features_knmi_agg


create schema if not exists features_hectopunten_agg;

create schema if not exists features_knmi_agg;