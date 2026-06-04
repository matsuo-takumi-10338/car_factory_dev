s3_source_path = "s3://databricks-sample-bucket-20260602/car_factory_dev/raw/"

checkpoint_path = "s3://databricks-sample-bucket-20260602/car_factory_dev/checkpoints/bronze/"

database_bronze_path = "s3://databricks-sample-bucket-20260602/car_factory_dev/database/bronze/"



bronze_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path + "/bronze")
    .option("header", "true")
    .load(s3_source_path))


query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True) 
    .option("path", database_bronze_path)
    .toTable("car_factory_dev.bronze.car_factory_bronze")) 