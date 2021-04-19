# Databricks notebook source
# scope='blobkey1', key='blobkey1', scope is from databricks, key is from key valut
# without password using the key
spark.conf.set(
"fs.azure.account.key.%s.blob.core.windows.net" % 'gk2dbazure',
dbutils.secrets.get(scope='blobkey1', key='gen2key'))

account_name='gk2dbazure'
movielens_container_name='movielens'
#relative_path='movies/movies.csv'
relative_path='movies'

movie_file_path = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format(movielens_container_name, account_name, relative_path)

print("Location ", movie_file_path)

moviesDf = spark.read.format('csv') \
              .option("header", True) \
              .option("inferSchema", True)\
              .option("sep", ",") \
              .load(movie_file_path)

moviesDf.show(2)
print(moviesDf.count())
# old one 9742

moviesDf.filter ('movieId >= 200001').show()

# COMMAND ----------

# abfs, abfss 
account_name='gk2dbazure'
movielens_container_name='movielens'
#relative_path='movies/movies.csv'
relative_path='movies'


spark.conf.set(
"fs.azure.account.key.%s.dfs.core.windows.net" % account_name,
dbutils.secrets.get(scope='blobkey1', key='gen2key'))

# note, protocol is abfs , azure blobs file system, DNS, blob to dfs [distributed file system]

# abfss://plantsense@trainingdemo3.dfs.core.windows.net
"""
input_file_location = "abfss://{0}@{1}.dfs.core.windows.net/{2}".format(blog_input_container_name, blog_account_name, blog_relative_path)
ouput_file_location = "abfss://{0}@{1}.dfs.core.windows.net/{2}".format(blog_output_container_name, blog_account_name, blog_output_path)
"""
 
# last char s is  for secured
movie_file_path = "abfss://{0}@{1}.dfs.core.windows.net/{2}".format(movielens_container_name, account_name, relative_path)

print("Location ", movie_file_path)

moviesDf = spark.read.format('csv') \
              .option("header", True) \
              .option("inferSchema", True)\
              .option("sep", ",") \
              .load(movie_file_path)

moviesDf.show(2)
print(moviesDf.count())
# old one 9742

filtered = moviesDf.filter ('movieId >= 200001')

filtered.show(2)

relative_path='movies_results/test.csv'

ouput_file_location = "abfss://{0}@{1}.dfs.core.windows.net/{2}".format(movielens_container_name, account_name, relative_path)
  
filtered\
.coalesce(1)\
.write\
.mode("overwrite")\
.option("header", "true")\
.format("csv")\
.save(ouput_file_location)\

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW top_movies AS select movieId, count(userId)  as total_rating, avg(rating) as avg_rating  from ratings group by movieId having avg_rating >= 4.0  order by total_rating desc 
# MAGIC 
# MAGIC  

# COMMAND ----------

resultsDf = spark.sql('select movieId, count(userId)  as total_rating, avg(rating) as avg_rating  from ratings group by movieId having avg_rating >= 4.0  order by total_rating desc')

resultsDf.show(10)

# COMMAND ----------


