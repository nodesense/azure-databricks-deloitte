CREATE DATABASE moviedb; 

use moviedb; 

/* describe csv file format, whenever new format/deviations found, create one format */

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
WITH (FORMAT_TYPE = DELIMITEDTEXT, 
FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)) 


CREATE EXTERNAL DATA SOURCE [tags_ds]
WITH (LOCATION = 'abfss://tags@gksynapsestorage.dfs.core.windows.net') 


CREATE EXTERNAL TABLE tags (userId INT, 
movieId INT, 
tag VARCHAR(256), 
timestamp BIGINT
) WITH (
LOCATION = 'tags.csv',
DATA_SOURCE = [tags_ds],
FILE_FORMAT = [ExternalCSVWithHeader]
)
 

select TOP 10 * from tags;

CREATE EXTERNAL DATA SOURCE [links_ds]
WITH (LOCATION = 'abfss://links@gksynapsestorage.dfs.core.windows.net') 
 
/* movieId,imdbId,tmdbId */

/* LOCATION  = '/' means all files in the folder shall be scanned ' */
 



CREATE EXTERNAL TABLE links (movieId INT, 
imdbId INT,
tmdbId INT
) WITH (
LOCATION = '/',
DATA_SOURCE = [links_ds],
FILE_FORMAT = [ExternalCSVWithHeader]
)
 
CREATE EXTERNAL DATA SOURCE [ratings_ds]
WITH (LOCATION = 'abfss://ratings@gksynapsestorage.dfs.core.windows.net') 
 
/*  userId,movieId,rating,timestamp  */

CREATE EXTERNAL TABLE ratings (userId INT, 
movieId INT,
rating FLOAT,
timestamp BIGINT
) WITH (
LOCATION = '/',
DATA_SOURCE = [ratings_ds],
FILE_FORMAT = [ExternalCSVWithHeader]
)



CREATE EXTERNAL DATA SOURCE [movies_ds]
WITH (LOCATION = 'abfss://movies@gksynapsestorage.dfs.core.windows.net') 
 
/*  movieId,title,genres  */



CREATE EXTERNAL TABLE movies (
movieId INT,
title VARCHAR(256),
genres VARCHAR(256)
) WITH (
LOCATION = '/',
DATA_SOURCE = [movies_ds],
FILE_FORMAT = [ExternalCSVWithHeader]
)

select   * from movies 


select   * from ratings 
/* LOCATION  = '/' means all files in the folder shall be scanned ' */


SELECT * FROM links;

SELECT * from tags;

SELECT * from sys.external_data_sources;

SELECT * from sys.external_file_formats;

SELECT * from sys.external_tables;

DROP EXTERNAL TABLE tags;

DROP EXTERNAL TABLE movies;

DROP EXTERNAL TABLE links;

DROP EXTERNAL TABLE ratings;

DROP EXTERNAL DATA SOURCE [tags_ds];

DROP EXTERNAL FILE  FORMAT [ExternalCSVWithHeader];

