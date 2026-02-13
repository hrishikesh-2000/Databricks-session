# Databricks notebook source
country = dbutils.jobs.taskValues.get("Ingest_data","country",debugValue="US")
print(country)