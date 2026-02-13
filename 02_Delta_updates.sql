-- Databricks notebook source
select * from demo.facts.sales;

-- COMMAND ----------

-- create intermediate table
create table demo.facts.sales_updates
as select * from demo.facts.sales;

-- COMMAND ----------

update  demo.facts.sales_updates set price  = price * 2 where id = 1;
select * from demo.facts.sales_updates;

-- COMMAND ----------

-- SCD 1
merge into demo.facts.sales tgt
using demo.facts.sales_updates src
on tgt.id = src.id
when matched
  then update
    set tgt.price = src.price

-- COMMAND ----------

select * from demo.facts.sales;

-- COMMAND ----------

-- SCD 2
alter table demo.facts.sales add column start_dte date, end_dte date;

-- COMMAND ----------

alter table demo.facts.sales_updates add column start_dte date, end_dte date;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr, col, current_date, uniform
-- MAGIC
-- MAGIC df = spark.table("demo.facts.sales") \
-- MAGIC     .withColumn("start_dte", expr("date_add(current_date(), cast(uniform(0, 365) as int))")) \
-- MAGIC     .withColumn("end_dte", expr("date_add(current_date(), cast(uniform(366, 730) as int))"))
-- MAGIC
-- MAGIC df.write.mode("overwrite").saveAsTable("demo.facts.sales")

-- COMMAND ----------

create or replace table demo.facts.sales_updates
as select * from demo.facts.sales;

-- COMMAND ----------

update  demo.facts.sales_updates set price  = price * 2 where id = 1;
select * from demo.facts.sales_updates;

-- COMMAND ----------

update  demo.facts.sales_updates set start_dte  = '2027-04-04', end_dte = '9999-12-31' where id = 1;
select * from demo.facts.sales_updates;

-- COMMAND ----------

update  demo.facts.sales_updates set start_dte  = '2026-06-16', end_dte = '2027-06-29' where id = 3;
update  demo.facts.sales_updates set start_dte  = '2026-09-20', end_dte = '2028-01-24' where id = 2;
select * from demo.facts.sales_updates;

-- COMMAND ----------

/*

1. Update end_date or target to start_date of new_row

1	Widget	79.96	5	2026-02-01	2026-02-13	2027-04-04

2. Insert the new row with new dates

*/

merge into demo.facts.sales tgt
using demo.facts.sales_updates src
on tgt.id = src.id 
  when matched and tgt.end_dte < src.end_dte
    then update set
      tgt.end_dte = src.start_dte

-- COMMAND ----------

select * from demo.facts.sales;

-- COMMAND ----------

merge into demo.facts.sales tgt
using demo.facts.sales_updates src
on tgt.id = src.id and tgt.price = src.price 
  when not matched
    then insert *

-- COMMAND ----------

