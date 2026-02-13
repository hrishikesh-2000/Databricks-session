# Databricks notebook source
dbutils.widgets.text("country","","Country")
country = dbutils.widgets.get("country")
print(country)

# COMMAND ----------

spark.sql("select * from samples.wanderbricks.bookings").display()

# COMMAND ----------

spark.sql("select * from samples.wanderbricks.properties").display()

# COMMAND ----------

spark.sql("select * from samples.wanderbricks.destinations").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_bookings = spark.sql("select * from samples.wanderbricks.bookings")

df_bookings_confirmed = df_bookings.filter(col("status") == "confirmed")

df_properties = spark.sql("select * from samples.wanderbricks.properties")

df_destinations = spark.sql("select * from samples.wanderbricks.destinations")

# COMMAND ----------

# DBTITLE 1,Cell 6
df = df_bookings_confirmed.alias("dbc").join(
    df_properties.alias("dp"), on=(col("dbc.property_id")==col("dp.property_id"))
).join(df_destinations.alias("dd"), on=(col("dp.destination_id")==col("dd.destination_id")))\
    .select("dbc.booking_id","dp.property_id","dd.destination_id","dd.country")

df.display()

# COMMAND ----------

df_country_filtered = df.filter(col("country") == 'United Arab Emirates')

df_country_filtered.display()

# COMMAND ----------

booking_count = df_country_filtered.count()

print(booking_count)
if booking_count < 1000:
    category = "COOL"
else:
    category = "HOT"

# COMMAND ----------

dbutils.jobs.taskValues.set("category", category)
dbutils.jobs.taskValues.set("country", country)