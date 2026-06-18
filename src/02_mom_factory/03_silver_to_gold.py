import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import dlt
from pyspark.sql import functions as F
from src.modules.env_params import EnvParams

env_params = EnvParams(domain="mom_factory", layer="gold")

slv_table_name = f"cf_mom_factory_{env_params.env}.silver.slv_mom_factory"
gld_schema_name = f"cf_mom_factory_{env_params.env}.gold"

# マスターテーブルの読み込み
m_operator_resource = spark.read.table(f"cf_mom_factory_{env_params.env}.master.m_operator_resource")
m_factory = spark.read.table(f"cf_mom_factory_{env_params.env}.master.m_factory")
m_process = spark.read.table(f"cf_mom_factory_{env_params.env}.master.m_process")
m_car_model = spark.read.table(f"cf_mom_factory_{env_params.env}.master.m_car_model")
m_production_standard = spark.read.table(f"cf_engineering_{env_params.env}.master.m_production_standard")

# =====================================================================
# ゴールド-BI表示用ライン別・車種別日次集計サマリー
# gld_bi_mom_factory
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_bi_mom_factory", 
    comment="ゴールド-BI表示用ライン別・車種別日次集計サマリー"
)
def gld_bi_mom_factory():
    slv_df = dlt.read(slv_table_name) 
    
    summary_df = (
        slv_df
        .withColumn("work_date", F.to_date(F.col("timestamp")))
        .groupBy("work_date", "factory_id", "line_id", "process_code", "car_model_code")
        .agg(
            F.count(F.col("vin")).alias("total_production_cnt"),
            F.sum(F.when(F.col("process_result_code") == 1, 1).otherwise(0)).alias("good_production_cnt"),
            F.sum(F.when(F.col("process_result_code") != 1, 1).otherwise(0)).alias("defect_production_cnt"),
            F.round(F.avg(F.col("cycle_time_sec")), 1).alias("avg_cycle_time_sec"),
            F.max(F.col("cycle_time_sec")).alias("max_cycle_time_sec"),
            F.sum(F.when(F.col("4m_changed_flg"), 1).otherwise(0)).alias("4m_change_event_cnt")
        )
        .withColumn(
            "yield_rate", 
            F.round(F.col("good_production_cnt") / F.col("total_production_cnt") * 100, 2)
        )
    )
    
    final_bi_df = (
        summary_df
        .join(m_factory.select("factory_id", "factory_name"), on="factory_id", how="left")
        .join(m_process.select("process_code", "process_disp_name", "sort_order"), on="process_code", how="left")
        .join(m_car_model.select("car_model_code", "car_model_name", "powertrain_type"), on="car_model_code", how="left")
    )
    
    return final_bi_df


# =====================================================================
# ゴールド-API用車両製造履歴検索
# gld_api_vehicle_trace	
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_api_vehicle_trace",
    comment="ゴールド-API用車両製造履歴検索",
    table_properties={"pipelines.zorderColumns": "vin"}
)
def gld_api_vehicle_trace():
    slv_df = dlt.read(slv_table_name)
    
    joined_df = (
        slv_df
        .join(m_factory.select("factory_id", "factory_name", "location_name"), on="factory_id", how="left")
        .join(m_process.select("process_code", "process_name", "process_disp_name", "sort_order"), on="process_code", how="left")
        .join(m_car_model.select("car_model_code", "car_model_name", "powertrain_type", "segment_code"), on="car_model_code", how="left")
        .join(m_operator_resource.select("operator_id", "resource_type", "resource_name", "department_or_vendor"), on="operator_id", how="left")
        .join(m_production_standard.select("car_model_code", "process_code", "design_version", "bom_id", "target_cycle_time"), 
              on=["car_model_code", "process_code"], how="left")
    )
    
    final_df = joined_df.select(
        "vin",
        "timestamp",
        "factory_id",
        "factory_name",
        "location_name",
        "line_id",
        "process_code",
        "process_name",
        "process_disp_name",
        "sort_order",
        "car_model_code",
        "car_model_name",
        "powertrain_type",
        "segment_code",
        "operator_id",
        "resource_type",
        "resource_name",
        "department_or_vendor",
        "parts_lot_no",
        "recipe_id",
        "design_version",
        "bom_id",
        "target_cycle_time",
        "process_result_code",
        "defect_code",
        "4m_changed_flg"
    )
    
    return final_df

# =====================================================================
# ゴールド-API用ライン作業者管理
# gld_api_operator_status
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_api_operator_status",
    comment="ゴールド-API用ライン作業者管理",
    table_properties={"pipelines.zorderColumns": "operator_id"}
)
def gld_api_operator_status():
    slv_df = dlt.read(slv_table_name)
    
    joined_df = (
        slv_df
        .join(m_operator_resource.select("operator_id", "resource_type", "resource_name", "department_or_vendor"), on="operator_id", how="left")
        .join(m_factory.select("factory_id", "factory_name"), on="factory_id", how="left")
        .join(m_process.select("process_code", "process_disp_name"), on="process_code", how="left")
        .join(m_car_model.select("car_model_code", "car_model_name"), on="car_model_code", how="left")
        .join(m_production_standard.select("car_model_code", "process_code", "target_cycle_time"), on=["car_model_code", "process_code"], how="left")
    )
    
    final_df = joined_df.select(
        "operator_id",
        "timestamp",
        "resource_type",
        "resource_name",
        "department_or_vendor",
        "factory_id",
        "factory_name",
        "line_id",
        "process_code",
        "process_disp_name",
        "vin",
        "car_model_code",
        "car_model_name",
        "cycle_time_sec",
        "target_cycle_time",
        "4m_changed_flg"
    )
    
    return final_df


# =====================================================================
# ゴールド-API用設備保全管理
# gld_api_facility_maintenance	
# =====================================================================
@dlt.table(
    name=f"{gld_schema_name}.gld_api_facility_maintenance",
    comment="ゴールド-API用設備保全管理",
    table_properties={"pipelines.zorderColumns": "line_id,recipe_id"}
)
def gld_api_facility_maintenance():
    slv_df = dlt.read(slv_table_name)
    
    joined_df = (
        slv_df
        .join(m_factory.select("factory_id", "factory_name"), on="factory_id", how="left")
        .join(m_process.select("process_code", "process_disp_name", "sort_order"), on="process_code", how="left")
    )
    
    final_df = joined_df.select(
        "line_id",
        "recipe_id",
        "timestamp",
        "factory_id",
        "factory_name",
        "process_code",
        "process_disp_name",
        "sort_order",
        "cycle_time_sec",
        "process_result_code",
        "defect_code",
        "4m_changed_flg"
    )
    
    return final_df
