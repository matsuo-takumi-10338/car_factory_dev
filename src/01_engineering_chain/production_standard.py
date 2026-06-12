from src.modules.env_params import EnvParams
from pyspark.sql import functions as F

env_params = EnvParams(domain="engineering",layer="production_standard")

table_name = "cf_engineering_dev.master.m_production_standard"

# 読み込み側
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
    .load(env_params.get_path("s3_source_path"))
)

# メタデータ挿入
bronze_df = (raw_df
    .withColumn("_input_file_path", F.col("_metadata.file_path"))      
    .withColumn("_processed_timestamp", F.current_timestamp()) 
    .filter(~F.col("_metadata.file_path").contains("_rescued_data"))
)

# 書き込み側
query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", env_params.get_path("checkpoint_location"))
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)