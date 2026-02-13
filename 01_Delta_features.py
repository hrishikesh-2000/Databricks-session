# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists demo
# MAGIC managed location 'abfss://data@sadbdemo.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists demo.facts;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo.facts.sales
# MAGIC (
# MAGIC   id int,
# MAGIC   product string,
# MAGIC   price decimal(5,2),
# MAGIC   quantity int,
# MAGIC   sale_date date
# MAGIC )

# COMMAND ----------

from pyspark.sql import Row
from datetime import date

data = [
    Row(id=1, product="Widget", price=19.99, quantity=5, sale_date=date(2026, 2, 1)),
    Row(id=2, product="Gadget", price=29.99, quantity=3, sale_date=date(2026, 2, 1)),
    Row(id=3, product="Thingamajig", price=9.99, quantity=10, sale_date=date(2026, 2, 1))
]

df = spark.createDataFrame(data)
df.write.insertInto("demo.facts.sales", overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.facts.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo.facts.sales_parquet
# MAGIC using parquet
# MAGIC location 'abfss://data@sadbdemo.dfs.core.windows.net/sales_parquet'
# MAGIC as select * from demo.facts.sales
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.facts.sales_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC update demo.facts.sales_parquet set price = 30.00 where id = 1

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.sql("select * from demo.facts.sales_parquet")

df_updated = df.withColumn("price", when(col("id")==1, lit(30.00)).otherwise(col("price")))

# COMMAND ----------

df_updated.display()

# COMMAND ----------

df_updated.write.format("parquet").option("path",'abfss://data@sadbdemo.dfs.core.windows.net/sales_parquet_temp').saveAsTable("demo.facts.sales_parquet_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists demo.facts.sales_parquet_temp

# COMMAND ----------

# swap directories
old_directory = "abfss://data@sadbdemo.dfs.core.windows.net/sales_parquet"
new_directory = "abfss://data@sadbdemo.dfs.core.windows.net/sales_parquet_temp"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists demo.facts.sales_parquet

# COMMAND ----------

dbutils.fs.rm(old_directory, recurse=True)

# COMMAND ----------

dbutils.fs.mv(new_directory, old_directory, recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table demo.facts.sales_parquet
# MAGIC using parquet
# MAGIC location 'abfss://data@sadbdemo.dfs.core.windows.net/sales_parquet'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.facts.sales_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC # Updating table in delta

# COMMAND ----------

# MAGIC %sql
# MAGIC update demo.facts.sales set price = price * 1.1 where id  = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.facts.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo.facts.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.facts.sales version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table demo.facts.sales version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.facts.sales;