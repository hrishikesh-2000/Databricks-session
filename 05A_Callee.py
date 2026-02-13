# Databricks notebook source
dbutils.widgets.text("Continent","","Continent Name")

# COMMAND ----------

cont = dbutils.widgets.get("Continent")
print(cont)

# COMMAND ----------

df = spark.read.table("samples.bakehouse.sales_customers")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

df_cont_filtered = df.filter(col("continent") == cont)

df_cont_filtered.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists sales.continents;

# COMMAND ----------

df_cont_filtered.write.format("Delta").mode("overwrite").saveAsTable(f"sales.continents.{cont}")

# COMMAND ----------

rows_written = spark.read.table(f"sales.continents.{cont}").count()
print(f"Rows written : {rows_written}")

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.exit(rows_written)