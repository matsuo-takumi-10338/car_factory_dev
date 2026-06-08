from modules.env_params import EnvParams

env_params = EnvParams(layer="bronze")

# 読み込み側
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
    .option("cloudFiles.partitionColumns", "factory_id")
    .option("header", "true")
    .load(env_params.get_path("s3_source_path"))
)

# 書き込み側
query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", env_params.get_path("checkpoint_location"))
    .option("mergeSchema", "true")
    .option("path", env_params.get_path("data_path"))
    .trigger(availableNow=True)
    .toTable(env_params.get_path("table_name"))
)