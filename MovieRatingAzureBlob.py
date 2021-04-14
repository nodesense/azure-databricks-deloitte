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

moviesDf.show(2)

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

ratingDf.show(2)

# COMMAND ----------

from pyspark.sql.functions import col
# col is the function, to create/represent col in Data Frame
# using DF API
# returns a new data frame
df2 = ratingDf.filter ( col('rating') > 4)
df2.show(5)
print("df2 count", df2.count())

# COMMAND ----------

# ratings > 4 using partial SQL  where rating > 4
df2 = ratingDf.filter ("rating > 4") # sql inside the string
print("count ", df2.count())
df2.show(2)

# COMMAND ----------

# using functions, 50+ functions
# import individually and use them as function
from pyspark.sql.functions import col, asc, desc, count, avg
# or using alias name, used in industries, F is a alias name, it can be called any variable name
import pyspark.sql.functions as F
# inplace of col, we can us F.col

# COMMAND ----------

df2 = ratingDf.filter ( F.col('rating') > 4)
df2.show(5)

# COMMAND ----------

# select movidId, count(movieId) "total_ratings" from ratings group by movieId order by total_ratings

display(ratingDf)

# COMMAND ----------

# number of ratings for movie by various users
ratings_count = ratingDf\
                .groupBy('movieId')\
                .agg(F.count("userId"))\


ratings_count.printSchema()
ratings_count.show(2)

# COMMAND ----------

# Popular movies, which received more ratings from users

ratings_count = ratingDf\
                .groupBy('movieId')\
                .agg(F.count("userId"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(F.desc("total_ratings"))

ratings_count.printSchema()
ratings_count.show(2)

display(ratings_count)


# COMMAND ----------


ratings_count = ratingDf\
                .groupBy('movieId')\
                .agg(F.count("userId"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(F.desc("total_ratings"))\
                .filter("total_ratings >= 100")

ratings_count.printSchema()
ratings_count.show(2)

display(ratings_count)


# COMMAND ----------

# joins example
top_100_movies = ratingDf\
                .groupBy('movieId')\
                .agg(F.count("userId"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(F.desc("total_ratings"))\
                .filter("total_ratings >= 100")


most_popular_movies = top_100_movies\
                        .join(moviesDf, top_100_movies.movieId == moviesDf.movieId) 

# join and return all the colums

most_popular_movies.show(2)
most_popular_movies.printSchema()

# COMMAND ----------

top_100_movies = ratingDf\
                .groupBy('movieId')\
                .agg(F.count("userId"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(F.desc("total_ratings"))\
                .filter("total_ratings >= 100")

most_popular_movies = top_100_movies\
                        .join(moviesDf, top_100_movies.movieId == moviesDf.movieId)\
                        .select(moviesDf["movieId"], "title", "total_ratings")

most_popular_movies.show(10)
display(most_popular_movies)

# COMMAND ----------

# top rated, pupular movies  avg(rating) >= 4, and most popular too >= 100 ratings

top_rated = ratingDf\
                .groupBy('movieId')\
                .agg(F.count("userId").alias('total_ratings'), F.avg("rating").alias("avg_rating") )\
                .sort(F.desc("avg_rating"))\
                .filter("total_ratings >= 100 AND avg_rating >= 4")

display(top_rated)

top_rated_movies = top_rated\
                        .join(moviesDf, top_rated.movieId == moviesDf.movieId)\
                        .select(moviesDf["movieId"], "title", "total_ratings", "avg_rating")
display(top_rated_movies)

# COMMAND ----------


movielens_container_name='movielens'
account_name='gkdbazure'
relative_path='results3/top_rated.csv'

relative_file_path = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format(movielens_container_name, account_name, relative_path)

top_rated_movies\
                .coalesce(1)\
                .write\
                .mode('overwrite')\
                .option("header", "true")\
                .format("csv")\
                .save(relative_file_path)


# COMMAND ----------

# read the results from azure blob storage

movielens_container_name='movielens'
account_name='gkdbazure'
relative_path='results2/top_rated.csv' # folder path, spark can load all the partition files if any

relative_file_path = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format(movielens_container_name, account_name, relative_path)

print("Location ", relative_file_path)

resultDf = spark.read.format('csv') \
              .option("header", True) \
              .option("inferSchema", True)\
              .option("sep", ",") \
              .load(relative_file_path)

resultDf.printSchema()
resultDf.show(2)
display(resultDf)

# COMMAND ----------


