# Databricks notebook source
# MAGIC %sql
# MAGIC drop catalog transactions cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog org
# MAGIC managed location 'abfss://data@sadbdemo.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema org.employee
# MAGIC managed location 'abfss://data@sadbdemo.dfs.core.windows.net/'

# COMMAND ----------



# COMMAND ----------

dbutils.fs.mkdirs("abfss://extvolume@sadbdemo.dfs.core.windows.net/extvolume/landing/2026/01/25")
dbutils.fs.mkdirs("abfss://extvolume@sadbdemo.dfs.core.windows.net/extvolume/landing/2026/01/26")
dbutils.fs.mkdirs("abfss://extvolume@sadbdemo.dfs.core.windows.net/extvolume/landing/2026/01/27")
dbutils.fs.mkdirs("abfss://extvolume@sadbdemo.dfs.core.windows.net/extvolume/landing/2026/01/28")
dbutils.fs.mkdirs("abfss://extvolume@sadbdemo.dfs.core.windows.net/extvolume/landing/2026/01/29")
dbutils.fs.mkdirs("abfss://extvolume@sadbdemo.dfs.core.windows.net/extvolume/landing/2026/01/30")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/transactions/employee/ext_volume/checkpoint")

# COMMAND ----------

df = (
    spark.
    readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("pathGlobFilter","*.csv")
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
    .option("header","true")
    .option("cloudFiles.schemaLocation","/Volumes/org/employee/ext_volume/checkpoint/2")
    .load("/Volumes/org/employee/ext_volume/extvolume/landing/*")

)

# COMMAND ----------

from pyspark.sql.functions import *

(
    df
    .withColumn("__fileName",col("_metadata.file_name"))
    .writeStream
    .option("checkpointLocation","/Volumes/org/employee/ext_volume/checkpoint/2")
    .outputMode("append")
    .option("mergeSchema","true")
    .trigger(availableNow=True)
    .toTable("org.employee.logins2")
)

# COMMAND ----------

# %sql drop table if exists org.employee.logins

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from org.employee.logins2;
# MAGIC