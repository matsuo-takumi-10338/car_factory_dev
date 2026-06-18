import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from src.modules.env_params import EnvParams

env_params = EnvParams(domain="mom_factory", layer="silver")

brz_table_name = f"cf_mom_factory_{env_params.env}.bronze.brz_mom_factory"
slv_table_name = f"cf_mom_factory_{env_params.env}.silver.slv_mom_factory"


# =====================================================================
# ステップ1：書き込み先となるシルバーテーブルを定義
# =====================================================================
dlt.create_streaming_table(
    name=slv_table_name,
    comment="シルバー-車両製造実績"
)


# =====================================================================
# ステップ2：ブロンズテーブルからストリーム読み込みしてクレンジング（一時View化）
# =====================================================================
@dlt.view(name="v_slv_mom_factory_cleaned")
@dlt.expect_or_drop("valid_log_id", "log_id IS NOT NULL")
def v_slv_mom_factory_cleaned():
    
    bronze_df = dlt.read_stream(brz_table_name)
    
    return (
        bronze_df
        .filter(~F.col("_input_file_path").contains("_rescued_data"))
        .withColumn("factory_id", F.upper(F.col("factory_id")))
        .withColumn("line_id", F.upper(F.col("line_id")))
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy/M/d HH:mm"))
        .withColumn("process_result_code", F.col("process_result_code").cast(IntegerType()))
        .withColumn(
            "cycle_time_sec",
            F.when(F.col("cycle_time_sec") == "NONE", F.lit(None).cast(DoubleType()))
             .when(F.col("cycle_time_sec").cast(DoubleType()) < 0, F.lit(None).cast(DoubleType()))
             .otherwise(F.col("cycle_time_sec").cast(DoubleType()))
        )
        .withColumn("4m_changed_flg", F.col("4m_changed_flg").cast(BooleanType()))
        .select(
            "log_id", "vin", "timestamp", "factory_id", "line_id", "process_code",
            "car_model_code", "process_result_code", "cycle_time_sec", "defect_code",
            "operator_id", "parts_lot_no", "recipe_id", "4m_changed_flg", "sequence_no",
            "_input_file_path", "_processed_timestamp"
        )
    )


# =====================================================================
# ステップ3：sequence_no を使った「後勝ち重複排除」を徹底
# =====================================================================
dlt.create_auto_cdc_flow(
    target = slv_table_name,
    source = "v_slv_mom_factory_cleaned",
    keys = ["log_id"],
    sequence_by = F.col("sequence_no"),
    stored_as_scd_type = 1
)
