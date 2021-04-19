CREATE DATABASE ecommercedb; 

use ecommercedb; 

/* describe csv file format, whenever new format/deviations found, create one format */

CREATE EXTERNAL FILE FORMAT ExternalParquetFile  
WITH (  
    FORMAT_TYPE = PARQUET, 
   DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
    );  
 


CREATE EXTERNAL DATA SOURCE [invoices_parquet_ds]
WITH (LOCATION = 'abfss://invoices-parquet@gksynapsestorage.dfs.core.windows.net') 

    

CREATE EXTERNAL TABLE invoices_parquet (
    InvoiceNo VARCHAR(256),
    StockCode  VARCHAR(256),
    Description  VARCHAR(256),
    Quantity INT,
    InvoiceDate DATETIME,
    UnitPrice float,
    Customer VARCHAR(256),
    Country VARCHAR(256)
) WITH (
LOCATION = '/invoices-01.parquet',
DATA_SOURCE = [invoices_parquet_ds],
FILE_FORMAT = [ExternalParquetFile]
)
 


CREATE EXTERNAL TABLE invoices_parquet (
    InvoiceNo VARCHAR(256),
    StockCode  VARCHAR(256),
    Description  VARCHAR(256),
    Quantity INT,
    InvoiceDate DATETIME,
    UnitPrice float,
    Customer VARCHAR(256),
    Country VARCHAR(256)
) WITH (
LOCATION = '/invoices-01.parquet',
DATA_SOURCE = [invoices_parquet_ds],
FILE_FORMAT = [ExternalParquetFile]
)

select InvoiceNo, Customer  from invoices_parquet;


/* partition based 

invoices-parquet-countrywise
*/


CREATE EXTERNAL DATA SOURCE [invoices_parquet_countrywise_ds]
WITH (LOCATION = 'abfss://invoices-parquet-countrywise@gksynapsestorage.dfs.core.windows.net') 

    

CREATE EXTERNAL TABLE invoices_countrywise_parquet (
    InvoiceNo VARCHAR(256),
    StockCode  VARCHAR(256),
    Description  VARCHAR(256),
    Quantity INT,
    InvoiceDate DATETIME,
    UnitPrice float,
    Customer VARCHAR(256),
    Country VARCHAR(256)
) WITH (
LOCATION = '/invoices-countrywise-01.parquet',
DATA_SOURCE = [invoices_parquet_countrywise_ds],
FILE_FORMAT = [ExternalParquetFile]
)
 

 select TOP 10 [InvoiceNo] from invoices_countrywise_parquet where Country='Germany';
 


DROP EXTERNAL TABLE invoices_parquet;
