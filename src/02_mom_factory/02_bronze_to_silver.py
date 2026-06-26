import sys
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType

files_path = spark.conf.get("workspace_files_path")
mom_catalog_name = spark.conf.get("mom_catalog_name")

pipeline_id = spark.conf.get("pipelines.pipelineId", "cf-mom-factory-pipeline-dev")
cluster_id = spark.conf.get("spark.databricks.pipelines.updateId", "unknown_run_id")

sys.path.insert(0, files_path)

from src.modules.env_params import EnvParams
from src.modules.enums import Status, ErrorSeverity, QuarantineReasonCode

env_params = EnvParams(domain="mom_factory", layer="silver", catalog_name = mom_catalog_name)

brz_table_name = f"{mom_catalog_name}.bronze.brz_mom_factory"
slv_table_name = f"{mom_catalog_name}.silver.slv_mom_factory"
qt_table_name = f"{mom_catalog_name}.silver.slv_qt_mom_factory"


@dlt.view(name="v_brz_mom_factory_prepared")
def v_brz_mom_factory_prepared():
    stream_df = dlt.read_stream(brz_table_name)
    
    df_with_ts = (
        stream_df
        .withColumn("parsed_ts", F.to_timestamp(F.col("timestamp"), "yyyy/M/d HH:mm"))
        .withColumn("cast_cycle_time", F.col("cycle_time_sec").cast(DoubleType()))
    )

    is_invalid_cycle_time = (
        (F.col("cycle_time_sec") == "NONE") | 
        (F.col("cast_cycle_time") < 0) |
        (F.col("cast_cycle_time").isNull() & F.col("cycle_time_sec").isNotNull())
    )
    
    expectation_array = F.array(
        F.when(F.col("_rescued_data").isNotNull(), F.lit(QuarantineReasonCode.SCHEMA_VIOLATION.value)),
        F.when(F.col("log_id").isNull(), F.lit(QuarantineReasonCode.MISSING_PRIMARY_KEY.value)),
        F.when(F.col("parsed_ts").isNull(), F.lit(QuarantineReasonCode.TYPE_CAST_FAILURE.value)),
        F.when(is_invalid_cycle_time, F.lit(QuarantineReasonCode.INVALID_VALUE_RANGE.value))
    )
    
    reason_array = F.array(
        F.when(F.col("_rescued_data").isNotNull(), F.lit("Auto Loader rescued row")),
        F.when(F.col("log_id").isNull(), F.lit("log_id is null")),
        F.when(F.col("parsed_ts").isNull(), F.lit("timestamp parse failed")),
        F.when(is_invalid_cycle_time, F.lit("cycle_time_sec value out of range or invalid"))
    )
    
    return (
        df_with_ts
        .withColumn("active_expectations", F.array_remove(expectation_array, F.lit(None)))
        .withColumn("active_reasons", F.array_remove(reason_array, F.lit(None)))
        .withColumn("is_invalid", F.size(F.col("active_expectations")) > 0)
        .withColumn(
            "highest_severity",
            F.when(
                (F.col("_rescued_data").isNotNull()) | (F.col("log_id").isNull()),
                F.lit(ErrorSeverity.CRITICAL.value) # スキーマ破壊・主キー欠損
            )
            .when(
                df_with_ts.parsed_ts.isNull() | is_invalid_cycle_time,
                F.lit(ErrorSeverity.ERROR.value)    # 【伏線回収】日付エラーや値の異常値は、業務影響大のERROR
            )
            .otherwise(F.lit(ErrorSeverity.WARNING.value))
        )
        
        .withColumn("raw_struct", F.struct(*stream_df.columns))
    )

# =====================================================================
# ステップ2：異常系 隔離テーブルへの書き込み
# =====================================================================
@dlt.table(name=qt_table_name)
def qt_slv_mom_factory():
    prepared_df = dlt.read_stream("v_brz_mom_factory_prepared")
    invalid_df = prepared_df.filter(F.col("is_invalid") == True)

    payload_json = F.to_json(F.col("raw_struct"))
    
    return invalid_df.select(
        F.md5(payload_json).alias("record_id"),       
        payload_json.alias("original_payload"),
        F.col("active_expectations").alias("expectation_name"),
        F.col("active_reasons").alias("failure_reason"),
        F.col("highest_severity").alias("severity"),
        F.lit(Status.OPEN.value).alias("status"),
        F.col("_input_file_path").alias("input_file_path"),
        F.col("_processed_timestamp").alias("processed_timestamp"),
        F.current_timestamp().alias("ingest_timestamp"),
        F.lit(pipeline_id).alias("job_name"),
        F.lit(cluster_id).alias("job_run_id")
    )


# =====================================================================
# ステップ3：正常系View
# =====================================================================
@dlt.view(name="v_slv_mom_factory")
def v_slv_mom_factory():

    prepared_df = dlt.read_stream("v_brz_mom_factory_prepared")
    valid_df = prepared_df.filter(F.col("is_invalid") == False)
    
    cleaned_df = (
        valid_df
        .withColumn("factory_id", F.upper(F.col("factory_id")))
        .withColumn("line_id", F.upper(F.col("line_id")))
        .withColumn("timestamp", F.col("parsed_ts"))
        .withColumn("process_result_code", F.col("process_result_code").cast(IntegerType()))
        .withColumn("cycle_time_sec", F.col("cycle_time_sec").cast(DoubleType()))
        .withColumn("4m_changed_flg", F.col("4m_changed_flg").cast(BooleanType()))
    )
    
    return cleaned_df.select(
        "log_id", "vin", "timestamp", "factory_id", "line_id", "process_code",
        "car_model_code", "process_result_code", "cycle_time_sec", "defect_code",
        "operator_id", "parts_lot_no", "recipe_id", "4m_changed_flg", "sequence_no",
        "_input_file_path", "_processed_timestamp"
    )


# =====================================================================
# ステップ4：CDC / MERGE 
# =====================================================================
dlt.create_auto_cdc_flow(
    target=slv_table_name,
    source="v_slv_mom_factory",
    keys=["log_id"],
    sequence_by="sequence_no",
    stored_as_scd_type=1,
)
