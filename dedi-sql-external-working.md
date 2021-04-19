create table departments(name varchar(60), category varchar(50)) 

insert into departments(name, category) values('Sales', 'special')
insert into departments(name, category) values('Finance', 'extra')
insert into departments(name, category) values('Marketing', 'addon')

select * from departments;


CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Teast123###$';


CREATE DATABASE SCOPED CREDENTIAL AZURECre
WITH IDENTITY= 'SHARED ACCESS SIGNATURE',
SECRET  = '<<keyhere>>'



CREATE EXTERNAL DATA SOURCE [employee4] 
    WITH (LOCATION = 'abfss://employees@xyz.dfs.core.windows.net', TYPE=HADOOP,
          CREDENTIAL =  AZURECre)
 
CREATE EXTERNAL FILE FORMAT [ExternalFileFormat4] WITH
(
FORMAT_TYPE = DELIMITEDTEXT,
FORMAT_OPTIONS (FIELD_TERMINATOR=',',FIRST_ROW =2)
)

CREATE EXTERNAL TABLE employee2 (
employee_name VARCHAR(60),
department VARCHAR(60),
salary VARCHAR(60)
)
WITH (
LOCATION = 'employees.csv',
DATA_SOURCE = [employee4],
FILE_FORMAT = [ExternalFileFormat4],
REJECT_TYPE = VALUE,
	REJECT_VALUE = 0
)

select * from dbo.employee2;

select  * from dbo.employee2 as e inner join dbo.departments as d on e.department = 'Sales';


select t1.* 
from dbo.employee2 as t1
inner join dbo.departments as t2 on t1.department = t2.name 
