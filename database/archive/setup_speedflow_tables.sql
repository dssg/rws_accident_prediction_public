DROP TABLE rws_raw.speed_15min_resolution;
DROP TABLE rws_raw.flow_15min_resolution;

-- create tables for speed and flow
CREATE TABLE rws_raw.speed_15min_resolution(
	"measurementSiteReference" VARCHAR NOT NULL, 
	"measurementSiteVersion" VARCHAR NOT NULL, 
	index VARCHAR NOT NULL, 
	"periodStart" TIMESTAMP WITHOUT TIME ZONE, 
	"periodEnd" TIMESTAMP WITHOUT TIME ZONE, 
	"numberOfIncompleteInputs" VARCHAR, 
	"numberOfInputValuesused" VARCHAR NOT NULL, 
	"minutesUsed" VARCHAR, 
	"computationalMethod" VARCHAR, 
	"standardDeviation" decimal, 
	"supplierCalculatedDataQuality" VARCHAR, 
	"sCDQ_Low" VARCHAR, 
	"sCDQ_SD" VARCHAR, 
	"number_of_sCDQ" VARCHAR, 
	"dataError" VARCHAR NOT NULL, 
	"travelTimeType" VARCHAR, 
	"avgVehicleFlow" decimal, 
	"avgVehicleSpeed" decimal NOT NULL, 
	"avgTravelTime" VARCHAR, 
	"computationMethod" VARCHAR NOT NULL, 
	"measurementEquipmentTypeUsed" VARCHAR NOT NULL, 
	"measurementSiteName1" VARCHAR NOT NULL, 
	"measurementSiteName2" VARCHAR, 
	"measurementSiteNumberOfLanes" VARCHAR NOT NULL, 
	"measurementSiteIdentification" VARCHAR, 
	"measurementSide" VARCHAR, 
	accuracy VARCHAR NOT NULL, 
	period VARCHAR NOT NULL, 
	"specificLane" VARCHAR NOT NULL, 
	"specificVehicleCharacteristics" VARCHAR NOT NULL, 
	"startLocatieForDisplayLat" VARCHAR NOT NULL, 
	"startLocatieForDisplayLong" VARCHAR NOT NULL, 
	"LocationCountryCode" VARCHAR NOT NULL, 
	"LocationTableNumber" VARCHAR NOT NULL, 
	"LocationTableVersion" VARCHAR NOT NULL, 
	"alertCDirectionCoded" VARCHAR NOT NULL, 
	"specificLocation" VARCHAR NOT NULL, 
	"offsetDistance" VARCHAR NOT NULL, 
	"LOC_TYPE" VARCHAR NOT NULL, 
	"LOC_DES" VARCHAR NOT NULL, 
	"ROADNUMBER" VARCHAR NOT NULL, 
	"ROADNAME" VARCHAR, 
	"FIRST_NAME" VARCHAR NOT NULL, 
	"SECND_NAME" VARCHAR, 
	"messageType" VARCHAR, 
	"publicationTime" VARCHAR, 
	"deducedNoTrafficMinutes" VARCHAR NOT NULL, 
	carriageway VARCHAR NOT NULL, 
	"generatedSiteName" VARCHAR NOT NULL, 
	voorganger VARCHAR, 
	opvolger VARCHAR, 
	"reasonForDataError" VARCHAR, 
	"openlrCoordinateLatitude" VARCHAR, 
	"openlrCoordinateLongitude" VARCHAR, 
	"openlrSideOfRoad" VARCHAR, 
	"openlrOrientation" VARCHAR, 
	"openlrPositiveOffset" VARCHAR, 
	"openlrReferencePointCoordinateLatitudes" VARCHAR, 
	"openlrReferencePointCoordinateLongitudes" VARCHAR, 
	"openlrReferencePointFunctionalRoadClasses" VARCHAR, 
	"openlrReferencePointFormOfWays" VARCHAR, 
	"openlrReferencePointBearings" VARCHAR, 
	"openlrReferencePointLowestFRCToNextLRPoints" VARCHAR, 
	"openlrReferencePointDistanceToNextLRPoints" VARCHAR
);

CREATE TABLE rws_raw.flow_15min_resolution (
	"measurementSiteReference" VARCHAR NOT NULL, 
	"measurementSiteVersion" varchar NOT NULL, 
	index VARCHAR NOT NULL,  
	"periodStart" TIMESTAMP WITHOUT TIME ZONE, 
	"periodEnd" TIMESTAMP WITHOUT TIME ZONE, 
	"numberOfIncompleteInputs" varchar, 
	"numberOfInputValuesused" varchar NOT NULL, 
	"minutesUsed" varchar, 
	"computationalMethod" varchar, 
	"standardDeviation" decimal, 
	"supplierCalculatedDataQuality" varchar, 
	"sCDQ_Low" varchar, 
	"sCDQ_SD" varchar, 
	"number_of_sCDQ" varchar, 
	"dataError" varchar NOT NULL, 
	"travelTimeType" varchar, 
	"avgVehicleFlow" decimal NOT NULL, 
	"avgVehicleSpeed" decimal, 
	"avgTravelTime" varchar, 
	"computationMethod" VARCHAR NOT NULL, 
	"measurementEquipmentTypeUsed" VARCHAR NOT NULL, 
	"measurementSiteName1" VARCHAR NOT NULL, 
	"measurementSiteName2" varchar, 
	"measurementSiteNumberOfLanes" varchar NOT NULL, 
	"measurementSiteIdentification" varchar, 
	"measurementSide" VARCHAR, 
	accuracy varchar NOT NULL, 
	period varchar NOT NULL, 
	"specificLane" VARCHAR NOT NULL, 
	"specificVehicleCharacteristics" VARCHAR NOT NULL, 
	"startLocatieForDisplayLat" varchar NOT NULL, 
	"startLocatieForDisplayLong" varchar NOT NULL, 
	"LocationCountryCode" varchar NOT NULL, 
	"LocationTableNumber" varchar NOT NULL, 
	"LocationTableVersion" VARCHAR NOT NULL, 
	"alertCDirectionCoded" VARCHAR NOT NULL, 
	"specificLocation" varchar NOT NULL, 
	"offsetDistance" varchar NOT NULL, 
	"LOC_TYPE" VARCHAR NOT NULL, 
	"LOC_DES" VARCHAR NOT NULL, 
	"ROADNUMBER" VARCHAR NOT NULL, 
	"ROADNAME" VARCHAR, 
	"FIRST_NAME" VARCHAR NOT NULL, 
	"SECND_NAME" VARCHAR, 
	"messageType" varchar, 
	"publicationTime" varchar, 
	"deducedNoTrafficMinutes" varchar NOT NULL, 
	carriageway VARCHAR NOT NULL, 
	"Aandachtsoordeel" VARCHAR NOT NULL, 
	"AandachtsoordeelCategorieen" VARCHAR NOT NULL, 
	"generatedSiteName" VARCHAR NOT NULL, 
	voorganger varchar, 
	opvolger varchar, 
	"reasonForDataError" VARCHAR, 
	"openlrCoordinateLatitude" varchar, 
	"openlrCoordinateLongitude" varchar, 
	"openlrSideOfRoad" varchar, 
	"openlrOrientation" varchar, 
	"openlrPositiveOffset" varchar, 
	"openlrReferencePointCoordinateLatitudes" varchar, 
	"openlrReferencePointCoordinateLongitudes" varchar, 
	"openlrReferencePointFunctionalRoadClasses" varchar, 
	"openlrReferencePointFormOfWays" varchar, 
	"openlrReferencePointBearings" varchar, 
	"openlrReferencePointLowestFRCToNextLRPoints" varchar, 
	"openlrReferencePointDistanceToNextLRPoints" varchar
);
