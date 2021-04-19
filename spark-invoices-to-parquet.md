# cell 1

```python

import pyspark
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import col, count, sum

# inferSchema, it scan data and build schema automatically
 
schema = StructType()\
        .add("InvoiceNo", StringType(), True)\
        .add("StockCode", StringType(), True)\
        .add("Description", StringType(), True)\
        .add("Quantity", IntegerType(), True)\
        .add("InvoiceDate", DateType(), True)\
        .add("UnitPrice", DoubleType(), True)\
        .add("Customer", StringType(), True)\
        .add("Country", StringType(), True)



```



# cell 2

```python

input_file_path = 'abfss://invoices@gksynapsestorage.dfs.core.windows.net/'
invoicesDf = spark.read.format('csv') \
              .option("header", True) \
              .schema(schema)\
              .option("sep", ",") \
              .option("dateFormat", "MM/dd/yyyy HH:mm")\
              .load(input_file_path)

invoicesDf.show(2)

```

#cell 3

```python


output_file_path = 'abfss://invoices-parquet@gksynapsestorage.dfs.core.windows.net/invoices-01.parquet'

invoicesDf\
.coalesce(1)\
.write\
.mode("overwrite")\
.parquet(output_file_path)


```
