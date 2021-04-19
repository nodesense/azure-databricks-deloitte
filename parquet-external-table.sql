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
 

select InvoiceNo, Customer  from invoices_parquet;


DROP EXTERNAL TABLE invoices_parquet;
