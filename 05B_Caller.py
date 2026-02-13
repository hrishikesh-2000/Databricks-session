# Databricks notebook source
#Caller

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

rows_fetched = dbutils.notebook.run(
    "/Workspace/Users/vpanchal217_gmail.com#ext#@vpanchal217gmail.onmicrosoft.com/Callee",
    600,
    {
        "Continent" : "Oceania"
    }
)

# COMMAND ----------

print(rows_fetched)