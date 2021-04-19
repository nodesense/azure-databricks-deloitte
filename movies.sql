CREATE TABLE movies (
movieId INT,
title VARCHAR(256),
genres VARCHAR(256)
); 

insert into movies (movieId, title, genres) values (200001, 'New Year 2021', 'Comedy');

select * from movies;

delete from movies; 

COPY INTO dbo.movies (movieId default 0, title default 'unknown', genres default 'unknown')
FROM 'https://gksynapsestorage.dfs.core.windows.net/movies/movies.csv'
WITH (
FILE_TYPE = 'CSV',
ROWTERMINATOR = '0x0A', 
FIRSTROW=2
) 

