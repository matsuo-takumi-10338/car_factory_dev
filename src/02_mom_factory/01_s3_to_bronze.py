import sys

sys.path.insert(0, '/Workspace/Shared/cf_prod/files')

import dlt
from pyspark.sql import functions as F
from src.modules.env_params import EnvParams


env_params = EnvParams(domain="mom_factory", layer="bronze")
brz_table_name = f"cf_mom_factory_{env_params.env}.bronze.brz_mom_factory"

@dlt.table(
    name=brz_table_name,
    comment="ブロンズ-車両製造実績"
)
def brz_mom_factory():
    
    raw_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
        .option("header", "true")
        .load(env_params.get_path("s3_source_path"))
    )
    
    return (raw_df
        .withColumn("_input_file_path", F.col("_metadata.file_path"))      
        .withColumn("_processed_timestamp", F.current_timestamp()) 
    )