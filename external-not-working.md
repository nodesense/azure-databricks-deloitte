create database moviedb2_ex_blob;

use moviedb2_ex_blob;

/* to encrypt the keys stored in the Azure db */
CREATE MASTER KEY ENCRYPTION BY PASSWORD  = 'xxxxxxxxxxxxxx';

/* credential shall be envyrpted using above key */

CREATE DATABASE SCOPED CREDENTIAL Az_gk2dbazure_Blob
   WITH IDENTITY='SHARED ACCESS SIGNATURE',
   SECRET = '';


CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
WITH (FORMAT_TYPE = DELIMITEDTEXT, 
FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)) 

DROP EXTERNAL DATA SOURCE  movies_ds;

CREATE EXTERNAL DATA SOURCE [movies_ds]
WITH (LOCATION = 'abfss://movielens@dddddddd.dfs.core.windows.net', CREDENTIAL=Az_gk2dbazure_Blob)


CREATE EXTERNAL DATA SOURCE [movies_ds]
WITH (LOCATION = 'https://ddddd.blob.core.windows.net/movielens', CREDENTIAL=Az_gk2dbazure_Blob)


 /* https://dddddd.blob.core.windows.net/movielens/movies/movies.csv */


DROP EXTERNAL TABLE movies;

CREATE EXTERNAL TABLE movies (
movieId INT,
title VARCHAR(256),
genres VARCHAR(256)
) WITH (
LOCATION = '/movies/movies.csv',
DATA_SOURCE = [movies_ds],
FILE_FORMAT = [ExternalCSVWithHeader]
)

select top 5 * from movies;
