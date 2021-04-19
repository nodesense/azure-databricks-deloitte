
CREATE EXTERNAL DATA SOURCE [ratings_ds]
  WITH (TYPE=HADOOP, 
        LOCATION = 'abfss://ratings@gksynapsestorage.dfs.core.windows.net')

 
CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
    WITH (FORMAT_TYPE = DELIMITEDTEXT, 
          FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2))
 
 
CREATE EXTERNAL TABLE ratings (userId INT, 
                            movieId INT, 
                            rating float, 
                            timestamp BIGINT
                            ) WITH (
                                LOCATION = '/',
                                DATA_SOURCE = [ratings_ds],
                                FILE_FORMAT = [ExternalCSVWithHeader]
                            )


SELECT * from ratings;

select * from movies;

SELECT title, rating from ratings r inner join movies m on r.movieId=m.movieId;


