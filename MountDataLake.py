# Databricks notebook source
# Mount azure storage into DataBricks FS as extension

dbutils.fs.ls("/")

# COMMAND ----------


dbutils.fs.mount(
  source = "wasbs://movielens@gkdbazure.blob.core.windows.net",
  mount_point = "/mnt/movielens",
  extra_configs = {"fs.azure.account.key.%s.blob.core.windows.net" % 'gkdbazure':dbutils.secrets.get(scope = "blobkey1", key = "blobkey1")})


# COMMAND ----------

dbutils.fs.ls("/mnt/movielens")

# COMMAND ----------


