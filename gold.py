from pyspark import pipelines as dp
from pyspark.sql.functions import col,to_date,when,sum,count,upper


@dp.materialized_view
def gold_table():
    df = spark.read.table("order_silver")
    df1 = df.withColumn("delivered_status_cnt",when(upper(col("order_status"))=="DELIVERED",1).otherwise(0)).withColumn("pending_status_cnt",when(upper(col("order_status"))!="DELIVERED",1).otherwise(0)).groupBy(to_date(col("change_timestamp")).alias("date")).agg(count("*").alias("total_orders"),sum(col("amount")).alias("total_amount"),sum(col("delivered_status_cnt")).alias("delivered_status_count"),sum(col("pending_status_cnt")).alias("pending_status_count")).drop("delivered_status_cnt","pending_status_cnt")
    return df1
