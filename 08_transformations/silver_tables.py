from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


# Temporary view for joining customer and orders
@dp.view(
    comment="customer_orders table"
)
def vw_joined():
    df_orders = spark.read.table("orders_bronze")
    df_customer = spark.read.table("customer_bronze")
    
    df_join = df_orders.join(df_customer, on=df_orders.o_custkey == df_customer.c_custkey, how="left_outer")
    
    return df_join


# Materialized view for silver
@dp.table(
    table_properties={"quality": "silver"},
    comment="joined table",
    name="joined_silver"
)
def joined_silver():
    return spark.read.table("vw_joined").withColumn("insert_date", current_timestamp())
