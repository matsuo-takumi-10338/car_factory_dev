from src.modules.env_params import EnvParams
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime

env_params = EnvParams(domain="engineering", layer="production_standard")
table_name = f"cf_engineering_{env_params.env}.master.m_sp_production_standard"

checkpoint_path = f"{env_params.get_path('checkpoint_location')}_sp"

# Auto Loaderによる読み込み
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
    .load(env_params.get_path("s3_source_path"))
)

# 前処理
bronze_df = (raw_df
    .filter(~F.col("_metadata.file_path").contains("_rescued_data"))
    .withColumn("target_cycle_time", F.col("target_cycle_time").cast(DoubleType()))
    .withColumn("_input_file_path", F.col("_metadata.file_path"))
    .withColumn("_processed_timestamp", F.current_timestamp())
    .select(
        "car_model_code", "process_code", "design_version", 
        "bom_id", "target_cycle_time", "_input_file_path", "_processed_timestamp"
    )
)

def _upsert_process(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        return
    
    view_name = f"temp_batch_data_{batch_id}"
    micro_batch_df.createOrReplaceTempView(view_name)
    spark.sql(f"CALL cf_engineering_dev.master.sp_merge_upsert('{table_name}', '{view_name}')")
    spark.catalog.dropTempView(view_name)

query = (
    bronze_df.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .foreachBatch(_upsert_process)
    .start()
)

query.awaitTermination()
