from modules.env_params import EnvParams
from pyspark.sql import functions as F

env_params = EnvParams(layer="bronze")

# 読み込み側
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
    .option("cloudFiles.partitionColumns", "factory_id")
    .option("header", "true")
    .load(env_params.get_path("s3_source_path"))
)

# メタデータ挿入
bronze_df = (raw_df
    .withColumn("_input_file_path", F.col("_metadata.file_path"))      
    .withColumn("_processed_timestamp", F.current_timestamp()) 
)

# 書き込み側
query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", env_params.get_path("checkpoint_location"))
    .option("mergeSchema", "true")
    .option("path", env_params.get_path("data_path"))
    .trigger(availableNow=True)
    .toTable(env_params.get_path("table_name"))
)