import sys
import dlt
from pyspark.sql import functions as F

files_path = spark.conf.get("workspace_files_path")
mom_catalog_name = spark.conf.get("mom_catalog_name")

sys.path.insert(0, files_path)

from src.modules.env_params import EnvParams


env_params = EnvParams(
    domain="mom_factory", layer="bronze", catalog_name=mom_catalog_name
)
brz_table_name = f"{mom_catalog_name}.bronze.brz_mom_factory"


@dlt.table(name=brz_table_name, comment="ブロンズ-車両製造実績")
def brz_mom_factory():
    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", env_params.get_path("schema_location"))
        .option("header", "true")
        .load(env_params.get_path("s3_source_path"))
    )

    return raw_df.withColumn(
        "_input_file_path", F.col("_metadata.file_path")
    ).withColumn("_processed_timestamp", F.current_timestamp())
