-- Databricks notebook source
create external table demo.facts.sales_external
location 'abfss://data@sadbdemo.dfs.core.windows.net/sales_external'
as select * from demo.facts.sales;

-- COMMAND ----------

select * from demo.facts.sales_external;

-- COMMAND ----------

describe demo.facts.sales_external;

-- COMMAND ----------

insert into demo.facts.sales_external (id, product, price, quantity, sale_date, start_dte, end_dte) values
  (5, 'Widget A', 19.99, 3, date'2026-02-01', date'2026-02-01', date'2026-02-28'),
  (6, 'Widget B', 29.99, 2, date'2026-02-02', date'2026-02-02', date'2026-02-28'),
  (7, 'Widget C', 15.50, 5, date'2026-02-03', date'2026-02-03', date'2026-02-28');


-- COMMAND ----------

  insert into demo.facts.sales_external (id, product, price, quantity, sale_date, start_dte, end_dte) values
  (8, 'Widget D', 45.00, 1, date'2026-02-04', date'2026-02-04', date'2026-02-28'),
  (9, 'Widget E', 12.75, 4, date'2026-02-05', date'2026-02-05', date'2026-02-28'),
  (10, 'Widget F', 22.00, 6, date'2026-02-06', date'2026-02-06', date'2026-02-28');

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("alter table demo.facts.sales_external set tblproperties ('delta.enableDeletionVectors' = false)")

-- COMMAND ----------

describe detail demo.facts.sales_external;

-- COMMAND ----------

delete from demo.facts.sales_external where id = 1

-- COMMAND ----------

update demo.facts.sales_external set price = price * 3 where id  = 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("alter table demo.facts.sales_external set tblproperties ('delta.enableDeletionVectors' = True)")

-- COMMAND ----------

delete from demo.facts.sales_external where id = 2

-- COMMAND ----------

describe history demo.facts.sales_external;

-- COMMAND ----------

optimize demo.facts.sales
zorder by id

-- COMMAND ----------

-- DBTITLE 1,Untitled
-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

-- COMMAND ----------

vacuum demo.facts.sales_external retain 0 hours;