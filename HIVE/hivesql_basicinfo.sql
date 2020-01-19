-------------------------------------------------
-- 1ST PASS SQL SCRIPT TO ANALYZE NEW EARTHQUAKES
-------------------------------------------------

-- HELPER TAB NORMALIZED EARTHQUAKE - downloaded EARTHQUAKEs LOADER + NORMALIZE text: TIME AND DATE

ADD JAR hdfs:///project/hiveudf/earthquakeDate.jar;
ADD JAR hdfs:///project/hiveudf/earthquakeTime.jar;

CREATE TEMPORARY FUNCTION extractTime as 'com.tiziano.onlyTime';
CREATE TEMPORARY FUNCTION extractDate as 'com.tiziano.onlyDate';

drop table if exists earthquakepartial;

create table earthquakepartial 
( 
  time string,
  latitude double,
  longitude double,
  depth double,
  mag double,
  magType string,
  nst string,
  gap string,
  dmin string,
  rms string,
  net string,
  id string,
  updated string,
  place string,
  type string,
  horizontalError string,
  depthError string,
  magError string,
  status string,
  locationSource string,
  magSource string
)
row format delimited fields terminated by ','
;

LOAD DATA INPATH '/project/datasets/earth2014.csv' OVERWRITE INTO TABLE earthquakepartial ;



drop table if exists earthquakepartial_normalized;

create table earthquakepartial_normalized 
( 
  id string,
  time string,
  day date,
  latitude double,
  longitude double,
  magnitude double
)
row format delimited fields terminated by ',';

INSERT OVERWRITE TABLE earthquakepartial_normalized SELECT id as id, extractTime(time) as time, extractDate(time) as day, latitude, longitude, mag as magnitude FROM earthquakepartial WHERE time NOT LIKE 'time';

-- HELPER TAB - FOR EACH CITY CALCULATE the DISTANCE FROM THE EARTHQUAKE 

drop table if exists earthquakepartial_normalized_distanceCityAll;

create table earthquakepartial_normalized_distanceCityAll 
( 
  id string,
  time string,
  day date,
  latitude double,
  longitude double,
  magnitude double,
  city string,
  country string,
  population int,
  city_distance double
)
row format delimited fields terminated by ',';

INSERT OVERWRITE TABLE earthquakepartial_normalized_distanceCityAll 
SELECT e.id as id, e.time as time, e.day as day, e.latitude as latitude, e.longitude as longitude, e.magnitude as magnitude, c.city as city, c.country as country, c.population as population,
60*1.1515*(180*(acos(((sin(radians(e.latitude))*sin(radians(c.latitude)))+(cos(radians(e.latitude))*cos(radians(c.latitude))*cos(radians(e.longitude-c.longitude))))))/PI()) as city_distance 
FROM earthquakepartial_normalized e 
CROSS JOIN cities_world c;


-- HELPER TAB : IDENTIFY THE CLOSEST CITY

drop table if exists earthquakepartial_normalized_distanceCityMinimum;

create table earthquakepartial_normalized_distanceCityMinimum
( 
  id string,
  time string,
  day date,
  latitude double,
  longitude double,
  magnitude double,
  city string,
  country string,
  population int,
  city_distance double
)
row format delimited fields terminated by ',';

INSERT OVERWRITE TABLE earthquakepartial_normalized_distanceCityMinimum 
SELECT 
e.id as id, e.time as time, e.day as day, e.latitude as latitude, e.longitude as longitude, 
e.magnitude as magnitude, e.city as city, e.country as country, e.population as population, e.city_distance as citydistance
FROM earthquakepartial_normalized_distanceCityAll e INNER JOIN
    (
        SELECT id, MIN(city_distance) as MinDistance
        FROM earthquakepartial_normalized_distanceCityAll
        GROUP BY id
    ) t ON e.id = t.id AND e.city_distance = t.MinDistance;


-- HELPER TAB : CALCULATE SEISMIC STATION DISTANCE FROM THE EARTHQUAKE  & CLOSEST CITY

ADD JAR hdfs:///project/hiveudf/deletecommas.jar;

CREATE TEMPORARY FUNCTION cleanLocation as 'com.tiziano.cleanLocation';

drop table if exists earthquakepartial_normalized_distanceCitySeismoALL;

create table earthquakepartial_normalized_distanceCitySeismoALL
( 
  id string,
  time string,
  day date,
  latitude double,
  longitude double,
  magnitude double,
  city string,
  country string,
  population int,
  city_distance double,
  seismocode string,
  seismoplace string,
  seismo_distance double
)
row format delimited fields terminated by ',';

INSERT OVERWRITE TABLE earthquakepartial_normalized_distanceCitySeismoALL 

SELECT e.id as id, e.time as time, e.day as day, e.latitude as latitude, e.longitude as longitude, e.magnitude as magnitude, 
e.city as city, e.country as country, e.population as population, e.city_distance as city_distance,
s.code as seismocode, cleanLocation(s.place) as seismoplace,
60*1.1515*(180*(acos(((sin(radians(e.latitude))*sin(radians(s.latitude)))+(cos(radians(e.latitude))*cos(radians(s.latitude))*cos(radians(e.longitude-s.longitude))))))/PI()) as seismo_distance 
FROM earthquakepartial_normalized_distancecityminimum e 
CROSS JOIN seismographs_stations s;

-- FINAL TAB -   IDENTIFY CLOSEST SEIMIC SENSOR

drop table if exists earthquakepartial_normalized_distanceCitySeismoMinimum;

create table earthquakepartial_normalized_distanceCitySeismoMinimum
( 
  id string,
  time string,
  day date,
  latitude double,
  longitude double,
  magnitude double,
  city string,
  country string,
  population int,
  city_distance double,
  seismocode string,
  seismoplace string,
  seismo_distance double
)
row format delimited fields terminated by ',';

INSERT OVERWRITE TABLE earthquakepartial_normalized_distanceCitySeismoMinimum 
SELECT 
e.id as id, e.time as time, e.day as day, e.latitude as latitude, e.longitude as longitude, e.magnitude as magnitude, 
e.city as city, e.country as country, e.population as population, e.city_distance as city_distance,
e.seismocode as seismocode,e.seismoplace as seismoplace, e.seismo_distance as seismo_distance
FROM earthquakepartial_normalized_distanceCitySeismoALL e INNER JOIN¬
    (
        SELECT id, MIN(seismo_distance) as MinDistance
        FROM earthquakepartial_normalized_distanceCitySeismoALL
        GROUP BY id
    ) t ON e.id = t.id AND e.seismo_distance = t.MinDistance;


--- delete helper tabs  --
drop table if exists earthquakepartial;
drop table if exists earthquakepartial_normalized;
drop table if exists earthquakepartial_normalized_distanceCityAll;
drop table if exists earthquakepartial_normalized_distanceCityMinimum;
drop table if exists earthquakepartial_normalized_distanceCitySeismoALL;
