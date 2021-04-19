```sql
CREATE DATABASE moviedb; 

use moviedb; 

CREATE EXTERNAL DATA SOURCE [tags_ds]
WITH (LOCATION = 'abfss://tags@gksynapsestorage.dfs.core.windows.net') 

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
WITH (FORMAT_TYPE = DELIMITEDTEXT, 
FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)) 




CREATE EXTERNAL TABLE tags (userId INT, 
movieId INT, 
tag VARCHAR(256), 
timestamp BIGINT
) WITH (
LOCATION = 'tags.csv',
DATA_SOURCE = [tags_ds],
FILE_FORMAT = [ExternalCSVWithHeader]
)

SELECT * from tags;

SELECT * from sys.external_data_sources;

SELECT * from sys.external_file_formats;

SELECT * from sys.external_tables;

DROP EXTERNAL TABLE tags;

DROP EXTERNAL DATA SOURCE [tags_ds];

DROP EXTERNAL FILE  FORMAT [ExternalCSVWithHeader];

```
