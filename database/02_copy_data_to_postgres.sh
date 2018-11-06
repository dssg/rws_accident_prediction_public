#!/bin/bash

# Script to copy raw data into Postgres Table from the directory
# Run this script from the /mnt/data/shared folder
# Note the flow and speed files may take several hours to copy

cat ongevallen/ongevallen.csv | psql -c "\copy rws_raw.ongevallen from stdin with csv header DELIMITER ';' NULL AS 'NA'";

cat hectopunten/hectopunten.csv | psql -c "\copy rws_raw.hectopunten from stdin with csv header DELIMITER ';' NULL AS 'NA'";

# The knmi_data.text file contained two tables. Therefore we manually extracted the two tables and created a knmi_data_clean.csv file (weather data) and a knmi_stn.csv file( metadata of each KNMI station). We removed trailing whitespace and changed the delimiter to ",".  

cat weather/knmi_data_clean.csv | psql -c "\copy rws_raw.knmi_data from stdin with csv header DELIMITER ',' NULL AS '' ";


cat weather/knmi_stn.csv | psql -c "\copy rws_raw.knmi_station from stdin with csv header DELIMITER ',' NULL AS '' ";

# copy flow data in loop
 for file in flow_and_speed/*_intensiteit_*.csv do
	#cat "$file" | psql "\copy rws_raw.flow_15min_resolution from stdin with csv header DELIMITER ',' NULL AS 'NA'";
	echo $file
done

for file in flow_and_speed/*_snelheid_*.csv do
 	#cat "$file" | psql "\copy rws_raw.speed_15min_resolution from stdin with csv header DELIMITER ',' NULL AS 'NA'";
done