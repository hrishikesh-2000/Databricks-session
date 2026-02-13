from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, current_timestamp


# Aggregate and store in gold
@dp.table(
    table_properties={"quality": "gold"},
    comment="orders_aggregated"
)
def gold_orders_agg():
    df = spark.read.table("joined_silver")
    
    df_final = df.groupBy(col("c_mktsegment")).agg(
        count(col("o_orderkey")).alias("total_orders")
    ).withColumn("insert_date", current_timestamp())
    
    return df_final
