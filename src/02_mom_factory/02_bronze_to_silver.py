import sys
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType

files_path = spark.conf.get("workspace_files_path")
mom_catalog_name = spark.conf.get("mom_catalog_name")


sys.path.insert(0, files_path)

from src.modules.env_params import EnvParams
from src.modules.enums import Status, ErrorSeverity, QuarantineReasonCode

env_params = EnvParams(
    domain="mom_factory", layer="silver", catalog_name=mom_catalog_name
)

brz_table_name = f"{mom_catalog_name}.bronze.brz_mom_factory"
slv_table_name = f"{mom_catalog_name}.silver.slv_mom_factory"
qt_table_name = f"{mom_catalog_name}.silver.slv_qt_mom_factory"


# =====================================================================
# ステップ1：ブロンズからストリームを引き、全エラーを一斉検知
# =====================================================================
@dlt.view(name="v_brz_mom_factory_prepared")
def v_brz_mom_factory_prepared():
    stream_df = dlt.read_stream(brz_table_name)

    meta_columns = {"_input_file_path", "_processed_timestamp", "_metadata"}
    business_columns = [col for col in stream_df.columns if col not in meta_columns]

    df_with_ts = stream_df.withColumn(
        "parsed_ts", F.to_timestamp(F.col("timestamp"), "yyyy/M/d HH:mm")
    ).withColumn("cast_cycle_time", F.col("cycle_time_sec").cast(DoubleType()))

    is_schema_violation = F.col("_rescued_data").isNotNull()
    is_missing_primary_key = F.col("log_id").isNull() | (F.col("log_id") == "")
    is_type_cast_failure = F.col("parsed_ts").isNull()
    is_invalid_cycle_time = (
        (F.col("cycle_time_sec") == "NONE")
        | (F.col("cast_cycle_time") < 0)
        | (F.col("cast_cycle_time").isNull() & F.col("cycle_time_sec").isNotNull())
    )

    is_invalid_row = (
        is_schema_violation
        | is_missing_primary_key
        | is_type_cast_failure
        | is_invalid_cycle_time
    )

    expectation_array = F.array(
        F.when(is_schema_violation, F.lit(QuarantineReasonCode.SCHEMA_VIOLATION.value)),
        F.when(
            is_missing_primary_key,
            F.lit(QuarantineReasonCode.MISSING_PRIMARY_KEY.value),
        ),
        F.when(
            is_type_cast_failure, F.lit(QuarantineReasonCode.TYPE_CAST_FAILURE.value)
        ),
        F.when(
            is_invalid_cycle_time, F.lit(QuarantineReasonCode.INVALID_VALUE_RANGE.value)
        ),
    )

    reason_array = F.array(
        F.when(is_schema_violation, F.lit("Auto Loader rescued row")),
        F.when(is_missing_primary_key, F.lit("log_id is null or empty")),
        F.when(is_type_cast_failure, F.lit("timestamp parse failed")),
        F.when(
            is_invalid_cycle_time, F.lit("cycle_time_sec value out of range or invalid")
        ),
    )

    return (
        df_with_ts.withColumn("active_expectations", F.array_compact(expectation_array))
        .withColumn("active_reasons", F.array_compact(reason_array))
        .withColumn("is_invalid", is_invalid_row)
        .withColumn(
            "highest_severity",
            F.when(
                is_schema_violation | is_missing_primary_key,
                F.lit(ErrorSeverity.CRITICAL.value),
            )
            .when(
                is_type_cast_failure | is_invalid_cycle_time,
                F.lit(ErrorSeverity.ERROR.value),
            )
            .otherwise(F.lit(ErrorSeverity.WARNING.value)),
        )
        .withColumn("raw_struct", F.struct(*business_columns))
    )


# =====================================================================
# ステップ2：異常系View
# =====================================================================
@dlt.view(name="v_qt_mom_factory")
def v_qt_mom_factory():
    prepared_df = dlt.read_stream("v_brz_mom_factory_prepared")
    invalid_df = prepared_df.filter(F.col("is_invalid") == True)

    payload_json = F.to_json(F.col("raw_struct"))

    runtime_pipeline_id = spark.conf.get(
        "spark.databricks.pipelines.pipelineId", "cf-mom-factory-pipeline"
    )
    runtime_update_id = spark.conf.get(
        "spark.databricks.pipelines.updateId", "unknown_run_id"
    )

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
        F.lit(runtime_pipeline_id).alias("job_name"),
        F.lit(runtime_update_id).alias("job_run_id"),
    )


# =====================================================================
# ステップ3：正常系View
# =====================================================================
@dlt.view(name="v_slv_mom_factory")
def v_slv_mom_factory():
    prepared_df = dlt.read_stream("v_brz_mom_factory_prepared")
    valid_df = prepared_df.filter(F.col("is_invalid") == False)

    cleaned_df = (
        valid_df.withColumn("factory_id", F.upper(F.col("factory_id")))
        .withColumn("line_id", F.upper(F.col("line_id")))
        .withColumn("timestamp", F.col("parsed_ts"))
        .withColumn(
            "process_result_code", F.col("process_result_code").cast(IntegerType())
        )
        .withColumn("cycle_time_sec", F.col("cycle_time_sec").cast(DoubleType()))
        .withColumn("4m_changed_flg", F.col("4m_changed_flg").cast(BooleanType()))
    )

    return cleaned_df.select(
        "log_id",
        "vin",
        "timestamp",
        "factory_id",
        "line_id",
        "process_code",
        "car_model_code",
        "process_result_code",
        "cycle_time_sec",
        "defect_code",
        "operator_id",
        "parts_lot_no",
        "recipe_id",
        "4m_changed_flg",
        "sequence_no",
        "_input_file_path",
        "_processed_timestamp",
    )


# =====================================================================
# ステップ4：CDC / MERGE（宛先テーブルの器作成とマージ処理）
# =====================================================================
dlt.create_streaming_table(name=slv_table_name, comment="シルバー-車両製造実績")

dlt.create_auto_cdc_flow(
    target=slv_table_name,
    source="v_slv_mom_factory",
    keys=["log_id"],
    sequence_by="sequence_no",
    stored_as_scd_type=1,
)

# =====================================================================
# ステップ5：異常系 隔離テーブルへの CDC / MERGE
# =====================================================================
dlt.create_streaming_table(name=qt_table_name, comment="シルバー-隔離用")

dlt.create_auto_cdc_flow(
    target=qt_table_name,
    source="v_qt_mom_factory",
    keys=["record_id"],
    sequence_by="processed_timestamp",
    stored_as_scd_type=1,
)
