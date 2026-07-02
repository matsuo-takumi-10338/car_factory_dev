import sys
import dlt
from pyspark.sql import functions as F

files_path = spark.conf.get("workspace_files_path")
mom_catalog_name = spark.conf.get("mom_catalog_name")
eng_catalog_name = spark.conf.get("eng_catalog_name")
target_env = spark.conf.get("myapp.environment", "dev")

sys.path.insert(0, files_path)

from src.modules.env_params import EnvParams
from src.modules.enums import Status, ErrorSeverity

env_params = EnvParams(domain="mom_factory", layer="gold", catalog_name=mom_catalog_name)

slv_table_name = f"{mom_catalog_name}.silver.slv_mom_factory"
gld_schema_name = f"{mom_catalog_name}.gold"
gld_qt_table_name = f"{gld_schema_name}.gld_qt_mom_factory"


# =====================================================================
# マスタ結合 ＆ リレーション整合性検証関数
# =====================================================================
def append_masters_and_validate(df):
    """
    ストリーム用・バッチ用どちらのDataFrameが来ても、
    全く同じマスタ結合と整合性検証を適用して返す共通のヘルパー関数
    """
    m_factory = spark.read.table(f"{mom_catalog_name}.master.m_factory").alias("m_fact")
    m_process = spark.read.table(f"{mom_catalog_name}.master.m_process").alias("m_proc")
    m_car_model = spark.read.table(f"{mom_catalog_name}.master.m_car_model").alias("m_car")
    m_operator = spark.read.table(f"{mom_catalog_name}.master.m_operator_resource").alias("m_oper")
    m_standard = spark.read.table(f"{eng_catalog_name}.master.m_production_standard").alias("m_std")

    joined_df = (
        df
        .join(m_factory.select("factory_id", "factory_name", "location_name"), "factory_id", "left")
        .join(m_process.select("process_code", "process_name", "process_disp_name", "sort_order"), "process_code", "left")
        .join(m_car_model.select("car_model_code", "car_model_name", "powertrain_type", "segment_code"), "car_model_code", "left")
        .join(m_operator.select("operator_id", "resource_type", "resource_name", "department_or_vendor"), "operator_id", "left")
        .join(m_standard.select("car_model_code", "process_code", "design_version", "bom_id", "target_cycle_time"), ["car_model_code", "process_code"], "left")
    )

    # マスタ不整合の判定
    is_factory_error = F.col("factory_name").isNull()
    is_process_error = F.col("process_disp_name").isNull()
    is_car_model_error = F.col("car_model_name").isNull()
    is_operator_error = F.col("operator_id").isNotNull() & F.col("resource_name").isNull()
    is_standard_error = F.col("target_cycle_time").isNull()

    is_invalid_master = (is_factory_error | is_process_error | is_car_model_error | is_operator_error | is_standard_error)

    expectation_array = F.array(
        F.when(is_factory_error, F.lit("MISSING_FACTORY_MASTER")),
        F.when(is_process_error, F.lit("MISSING_PROCESS_MASTER")),
        F.when(is_car_model_error, F.lit("MISSING_CAR_MODEL_MASTER")),
        F.when(is_operator_error, F.lit("MISSING_OPERATOR_RESOURCE_MASTER")),
        F.when(is_standard_error, F.lit("MISSING_PRODUCTION_STANDARD_MASTER"))
    )

    return (
        joined_df
        .withColumn("gld_active_expectations", F.array_compact(expectation_array))
        .withColumn("is_gld_invalid", is_invalid_master)
    )


# =====================================================================
# 異常系隔離（CDC）に繋ぐための「ストリーム専用View」
# =====================================================================
@dlt.view(name="v_slv_mom_factory_validated_stream")
def v_slv_mom_factory_validated_stream():
    return append_masters_and_validate(dlt.read_stream(slv_table_name))


# =====================================================================
# 正常系BI/API（集計）に繋ぐための「バッチ専用View」
# =====================================================================
@dlt.view(name="v_slv_mom_factory_validated_batch")
def v_slv_mom_factory_validated_batch():
    return append_masters_and_validate(dlt.read(slv_table_name))


# =====================================================================
# ゴールド異常系：マスタ未登録データの隔離テーブル
# =====================================================================
dlt.create_streaming_table(name=gld_qt_table_name, comment="ゴールド-マスタ不整合データ隔離マスター")

@dlt.view(name="v_gld_qt_mom_factory")
def v_gld_qt_mom_factory():
    validated_stream = dlt.read_stream("v_slv_mom_factory_validated_stream")
    invalid_df = validated_stream.filter(F.col("is_gld_invalid") == True)
    
    business_columns = ["log_id", "vin", "timestamp", "factory_id", "line_id", "process_code", "car_model_code", "process_result_code", "cycle_time_sec", "defect_code", "operator_id", "parts_lot_no", "recipe_id", "4m_changed_flg", "sequence_no"]
    payload_json = F.to_json(F.struct(*business_columns))

    return invalid_df.select(
        F.col("log_id").alias("record_id"),
        payload_json.alias("original_payload"),
        F.col("gld_active_expectations").alias("expectation_name"),
        F.lit(ErrorSeverity.ERROR.value).alias("severity"),
        F.lit(Status.OPEN.value).alias("status"),
        F.col("_input_file_path").alias("input_file_path"),
        F.col("_processed_timestamp").alias("processed_timestamp"),
        F.current_timestamp().alias("ingest_timestamp"),
        F.lit(target_env).alias("env_name")
    )

dlt.create_auto_cdc_flow(
    target=gld_qt_table_name,
    source="v_gld_qt_mom_factory",
    keys=["record_id"],
    sequence_by="processed_timestamp",
    stored_as_scd_type=1,
)


# =====================================================================
# ゴールド-BI表示用ライン別・車種別日次集計サマリー
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_bi_mom_factory",
    comment="ゴールド-BI表示用ライン別・車種別日次集計サマリー",
)
def gld_bi_mom_factory():
    validated_df = dlt.read("v_slv_mom_factory_validated_batch")
    valid_df = validated_df.filter(F.col("is_gld_invalid") == False)

    summary_df = (
        valid_df.withColumn("work_date", F.to_date(F.col("timestamp")))
        .groupBy("work_date", "factory_id", "factory_name", "line_id", "process_code", "process_disp_name", "sort_order", "car_model_code", "car_model_name", "powertrain_type")
        .agg(
            F.count(F.col("vin")).alias("total_production_cnt"),
            F.sum(F.when(F.col("process_result_code") == 1, 1).otherwise(0)).alias("good_production_cnt"),
            F.sum(F.when(F.col("process_result_code") != 1, 1).otherwise(0)).alias("defect_production_cnt"),
            F.round(F.avg(F.col("cycle_time_sec")), 1).alias("avg_cycle_time_sec"),
            F.max(F.col("cycle_time_sec")).alias("max_cycle_time_sec"),
            F.sum(F.when(F.col("4m_changed_flg"), 1).otherwise(0)).alias("4m_change_event_cnt"),
            F.max(F.col("_processed_timestamp")).alias("_processed_timestamp"),
        )
        .withColumn(
            "yield_rate",
            F.round(F.col("good_production_cnt") / F.col("total_production_cnt") * 100, 2),
        )
    )
    return summary_df


# =====================================================================
# ゴールド-API用車両製造履歴検索
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_api_vehicle_trace",
    comment="ゴールド-API用車両製造履歴検索",
    table_properties={"pipelines.zorderColumns": "vin"},
)
def gld_api_vehicle_trace():
    validated_df = dlt.read("v_slv_mom_factory_validated_batch")
    valid_df = validated_df.filter(F.col("is_gld_invalid") == False)

    return valid_df.select(
        "vin", "timestamp", "factory_id", "factory_name", "location_name", "line_id",
        "process_code", "process_name", "process_disp_name", "sort_order",
        "car_model_code", "car_model_name", "powertrain_type", "segment_code",
        "operator_id", "resource_type", "resource_name", "department_or_vendor",
        "parts_lot_no", "recipe_id", "design_version", "bom_id", "target_cycle_time",
        "process_result_code", "defect_code", "4m_changed_flg", "_processed_timestamp"
    )


# =====================================================================
# ゴールド-API用ライン作業者管理
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_api_operator_status",
    comment="ゴールド-API用ライン作業者管理",
    table_properties={"pipelines.zorderColumns": "operator_id"},
)
def gld_api_operator_status():

    validated_df = dlt.read("v_slv_mom_factory_validated_batch")
    valid_df = validated_df.filter(F.col("is_gld_invalid") == False)

    return valid_df.select(
        "operator_id", "timestamp", "resource_type", "resource_name", "department_or_vendor",
        "factory_id", "factory_name", "line_id", "process_code", "process_disp_name",
        "vin", "car_model_code", "car_model_name", "cycle_time_sec", "target_cycle_time",
        "4m_changed_flg", "_processed_timestamp"
    )


# =====================================================================
# ゴールド-API用設備保全管理
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_api_facility_maintenance",
    comment="ゴールド-API用設備保全管理",
    table_properties={"pipelines.zorderColumns": "line_id,recipe_id"},
)
def gld_api_facility_maintenance():
    validated_df = dlt.read("v_slv_mom_factory_validated_batch")
    valid_df = validated_df.filter(F.col("is_gld_invalid") == False)

    return valid_df.select(
        "line_id", "recipe_id", "timestamp", "factory_id", "factory_name",
        "process_code", "process_disp_name", "sort_order", "cycle_time_sec",
        "process_result_code", "defect_code", "4m_changed_flg", "_processed_timestamp"
    )