CREATE DATABASE ecommercedb;

use ecommercedb;


CREATE EXTERNAL DATA SOURCE [customer_ds]
  WITH (LOCATION = 'abfss://customers@gksynapsestorage.dfs.core.windows.net')
 

CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
    WITH (FORMAT_TYPE = DELIMITEDTEXT, 
          FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2))

/* CustomerID,Country,Gender,Age */

CREATE EXTERNAL TABLE customers (CustomerID INT, 
                            Country VARCHAR(256), 
                            Gender VARCHAR(2), 
                            Age INT
                            ) WITH (
                                LOCATION = '/',
                                DATA_SOURCE = [customer_ds],
                                FILE_FORMAT = [ExternalCSVWithHeader]
                            )


SELECT * from customers;
