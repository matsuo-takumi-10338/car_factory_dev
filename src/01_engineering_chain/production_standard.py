from src.modules.env_params import EnvParams
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable  

env_params = EnvParams(domain="engineering", layer="production_standard")
table_name = f"cf_engineering_{env_params.env}.master.m_production_standard"

# 1. 読み込み側
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
    .load(env_params.get_path("s3_source_path"))
)

# 2. メタデータ挿入・型変換
bronze_df = (raw_df
    .filter(~F.col("_metadata.file_path").contains("_rescued_data"))
    .withColumn("target_cycle_time", F.col("target_cycle_time").cast(DoubleType()))
    .withColumn("_input_file_path", F.col("_metadata.file_path"))      
    .withColumn("_processed_timestamp", F.current_timestamp()) 
    .select(
        "car_model_code",
        "process_code",
        "design_version",
        "bom_id",
        "target_cycle_time",
        "_input_file_path",
        "_processed_timestamp"
    )
)

def upsert_production_standard(micro_batch_df, batch_id):
    target_delta_table = DeltaTable.forName(spark, table_name)
    (target_delta_table.alias("target")
     .merge(
         micro_batch_df.alias("source"),
         """
         target.car_model_code = source.car_model_code AND 
         target.process_code = source.process_code AND 
         target.design_version = source.design_version
         """
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )


query = (
    bronze_df.writeStream
    .option("checkpointLocation", env_params.get_path("checkpoint_location"))
    .trigger(availableNow=True)
    .foreachBatch(upsert_production_standard)
    .start()
)