CREATE TABLE [dbo].[DistributedEmployee2]
(  [employee_name]      nvarchar(20) NOT NULL
,   [department]      nvarchar(20) NOT NULL
,    [salary]            nvarchar(20)         NOT NULL
)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,  DISTRIBUTION = HASH([employee_name])
)
;

--Note when specifying the column list, input field numbers start from 1


COPY INTO dbo.DistributedEmployee2 
(employee_name default 'myStringDefault', department default 'myStringDefault' ,salary default '1.0')
FROM 'https://gksynapsestorage.blob.core.windows.net/employees/employees.csv'
WITH (
	FILE_TYPE = 'CSV',
    
    ROWTERMINATOR='0x0A' --0x0A specifies to use the Line Feed character (Unix based systems)
)

select * from dbo.DistributedEmployee2 ;
