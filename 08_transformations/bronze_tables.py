from pyspark import pipelines as dp

# Streaming table for orders
@dp.table(
    table_properties={"quality": "bronze"},
    comment="orders bronze table"
)
def orders_bronze():
    return spark.readStream.table("dlt_demo.bronze.orders")


# Materialized view for customer
@dp.table(
    table_properties={"quality": "bronze"},
    comment="customer bronze table"
)
def customer_bronze():
    return spark.read.table("dlt_demo.bronze.customer")
