# Databricks notebook source
# scope='blobkey1', key='blobkey1', scope is from databricks, key is from key valut
# without password using the key
spark.conf.set(
"fs.azure.account.key.%s.blob.core.windows.net" % 'gkdbazure',
dbutils.secrets.get(scope='blobkey1', key='blobkey1'))


movielens_container_name='movielens'
account_name='gkdbazure'
relative_path='movies.csv'

movie_file_path = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format(movielens_container_name, account_name, relative_path)

print("Location ", movie_file_path)

moviesDf = spark.read.format('csv') \
              .option("header", True) \
              .option("inferSchema", True)\
              .option("sep", ",") \
              .load(movie_file_path)


# COMMAND ----------


movielens_container_name='movielens'
account_name='gkdbazure'
relative_path='ratings.csv'

rating_file_path = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format(movielens_container_name, account_name, relative_path)

print("Location ", rating_file_path)

ratingDf = spark.read.format('csv') \
              .option("header", True) \
              .option("inferSchema", True)\
              .option("sep", ",") \
              .load(rating_file_path)


 
ratingDf.write.format("parquet").saveAsTable("tbl_ratings")

# COMMAND ----------

# spark allows to create sql view [read, no write], immutable
# createTempView, createGlobalTempView

# create data frame and register that data frame as SQL View
# writing code in SQL/DF, performance wise all same

ratingDf.createOrReplaceTempView("ratings")

# spark.sql to execute spark code
# or use %sql shell to execute code

ratingSQLDf = spark.sql("select * from ratings")
ratingSQLDf.printSchema()
ratingSQLDf.show(2)
display(ratingSQLDf)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select movieId, count(userId)  as total_rating, avg(rating) as avg_rating  from ratings group by movieId having avg_rating >= 4.0  order by total_rating desc 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW top_rated_view  as select movieId, count(userId)  as total_rating, avg(rating) as avg_rating  from ratings group by movieId having avg_rating >= 4.0  order by total_rating desc 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from top_rated_view

# COMMAND ----------

resultsDf = spark.sql('select movieId, count(userId)  as total_rating, avg(rating) as avg_rating  from ratings group by movieId having avg_rating >= 4.0  order by total_rating desc')

resultsDf.show(10)

# COMMAND ----------


