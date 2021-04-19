# Databricks notebook source
spark.conf.set(
"fs.azure.account.key.%s.blob.core.windows.net" % 'syndbstorage',
dbutils.secrets.get(scope='blobscope', key='key1'))

blog_input_container_name='movielens'
blog_account_name='syndbstorage'
blog_relative_path='movies/movies.csv'
input_file_location = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format(blog_input_container_name, blog_account_name, blog_relative_path)


print("input_file_location", input_file_location)

 
movies = spark.read.format('csv') \
.option("header", True) \
.option("inferSchema", True)\
.option("sep", ",") \
.load(input_file_location)

movies.show(2)

blog_relative_path = "ratings/ratings.csv"
input_file_location = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format(blog_input_container_name, blog_account_name, blog_relative_path)

ratings = spark.read.format('csv') \
.option("header", True) \
.option("inferSchema", True)\
.option("sep", ",") \
.load(input_file_location)
 
df.printSchema()

ratings.show(2)



# COMMAND ----------

from pyspark.sql.functions import asc, desc, avg, col, count, avg
most_popularly_rated = ratings \
                       .groupBy("movieId")\
                       .agg(count("userId")) \
                       .withColumnRenamed("count(userId)", "total_ratings")\
                       .sort(desc("total_ratings"))
display(most_popularly_rated)

# COMMAND ----------

# get the movies rated by more than >= 100 users
most_popularly_rated_top_rated_above_100_users = ratings \
                       .groupBy("movieId")\
                       .agg(count("userId")) \
                       .withColumnRenamed("count(userId)", "total_ratings")\
                       .sort(desc("total_ratings")) \
                       .filter(col("total_ratings") >= 100)

display(most_popularly_rated_top_rated_above_100_users)

# COMMAND ----------

most_popular_movies = most_popularly_rated_top_rated_above_100_users \
                                  .join(movies, most_popularly_rated_top_rated_above_100_users.movieId == movies.movieId )\
                                  .select(most_popularly_rated_top_rated_above_100_users["movieId"], "title", "total_ratings")

display(most_popular_movies)

# COMMAND ----------

 # top_rated
 # agg rating group by movieId on ratings df
#            .filter( ("total_ratings" >= 100) & ("avg_rating" >= 3) ) # FIXME

top_rated = ratings \
            .groupBy("movieId")\
            .agg(avg("rating").alias("avg_rating"), count("userId").alias("total_ratings"))\
            .sort(desc("avg_rating")) \
            .filter ("total_ratings >= 100 AND avg_rating >= 3")
top_rated.show(2)

# COMMAND ----------

top_rated_movies = top_rated.join(movies, top_rated.movieId == movies.movieId) \
                             .select(top_rated['movieId'], 'title', 'avg_rating', 'total_ratings')

display(top_rated_movies)

# COMMAND ----------

ratings.createTempView('ratings')
movies.createTempView('movies')

# COMMAND ----------

# MAGIC %sql select movieId, count(userId) as total_ratings from ratings   group by movieId having total_ratings > 100 order by total_ratings

# COMMAND ----------

spark.sql("select movieId, count(userId) as total_ratings from ratings   group by movieId having total_ratings > 100 order by total_ratings").explain(extended = True)

# COMMAND ----------

# MAGIC %sql select r.movieId, m.title, count(userId) as total_ratings from ratings r join movies m on (m.movieId = r.movieId)  group by r.movieId, m.title having total_ratings > 100 order by total_ratings

# COMMAND ----------

spark.sql("select r.movieId, m.title, count(userId) as total_ratings from ratings r join movies m on (m.movieId = r.movieId)  group by r.movieId, m.title having total_ratings > 100 order by total_ratings").explain()

# COMMAND ----------

spark.sql("select r.movieId, m.title, count(userId) as total_ratings from ratings r join movies m on (m.movieId = r.movieId)  group by r.movieId, m.title having total_ratings > 100 order by total_ratings").show(10)

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://movielens@syndbstorage.blob.core.windows.net",
  mount_point = "/mnt/ml",
  extra_configs = {"fs.azure.account.key.%s.blob.core.windows.net" % 'syndbstorage':dbutils.secrets.get(scope = "blobscope", key = "key1")})

dbutils.fs.ls("/mnt/ml")


# COMMAND ----------


file_location2 = 'dbfs:/mnt/ml/movies/movies.csv'
df2 = spark.read.format('csv') \
.option("header", True) \
.option("inferSchema", True)\
.option("sep", ",") \
.load(file_location2)

df.printSchema()


# COMMAND ----------

# MAGIC %sql CREATE DATABASE movielensdb LOCATION "wasbs://movielens@syndbstorage.blob.core.windows.net/movies";

# COMMAND ----------

spark.conf.set(
"spark.hadoop.fs.azure.account.key.syndbstorage.blob.core.windows.net",
"wFYpOfw+/Jfx ")



# COMMAND ----------

# MAGIC %sql CREATE DATABASE movielensdb LOCATION "wasbs://movielens@syndbstorage.blob.core.windows.net";

# COMMAND ----------


