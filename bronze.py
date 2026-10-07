from pyspark import pipelines as dp
from pyspark.sql.functions import col,current_timestamp,expr

@dp.table
def orders_bronze():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("multiline", "true").load("/Volumes/workspace/default/assets/CDC_DATA/*.json")
    df = df.withColumn("ingestion_date",current_timestamp()).withColumn("_source_file_path",col("_metadata.file_path")).withColumn("_source_file_name",col("_metadata.file_name")).withColumn("_source_file_offset",col("_metadata.file_block_start")).withColumn("_source_file_timestamp",col("_metadata.file_modification_time"))
    return df