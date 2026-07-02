import pytest
import sys
from datetime import datetime
from pyspark.sql import functions as F
import importlib

s3_to_bronze = importlib.import_module("src.02_mom_factory.01_s3_to_bronze")

TEST_S3_SOURCE_PATH = "s3://cf-mom-factory-bucket-dev/mom_factory_dev/tests/raw/01_s3_to_bronze"
TEST_SCHEMA_LOCATION = "s3://cf-mom-factory-bucket-dev/mom_factory_dev/tests/checkpoints/01_s3_to_bronze"
TEST_BRZ_TABLE       = "cf_mom_factory_dev_matsuot.bronze.brz_mom_factory_test"

# =====================================================================
# 1. Pytestのフィクスチャ（前準備：CSV配置 ➔ パイプライン実行 ➔ 後片付け）
# =====================================================================
@pytest.fixture(scope="module", autouse=True)
def setup_and_run_pipeline():
    """テスト実行前にS3へCSVを配置し、Auto Loaderを一発動かして結果をテーブルに格納する"""
    # 1. テスト用環境のクリーンアップ
    dbutils.fs.rm(TEST_S3_SOURCE_PATH, recurse=True)
    dbutils.fs.rm(TEST_SCHEMA_LOCATION, recurse=True)
    spark.sql(f"DROP TABLE IF EXISTS {TEST_BRZ_TABLE}")
    
    # 2. テスト用CSVデータの作成
    csv_content = """log_id,vin,timestamp,factory_id,line_id,process_code,car_model_code,process_result_code,cycle_time_sec,defect_code,operator_id,parts_lot_no,recipe_id,4m_changed_flg,sequence_no
F03-L0001,VIN-F03-00001,2026/6/26 10:00,F03,LINE-A,P05,BZ4X-EV,1,60.0,NONE,OP-106,LOT-BAT-101,RCP-01,FALSE,1
F03-L0002,VIN-F03-00002,2026/6/26 10:05,F03,LINE-A,P05,BZ4X-EV,1,55.5,NONE,OP-106,LOT-BAT-101,RCP-01,FALSE,2"""
    
    file_name = "test_source_vehicle_logs.csv"
    dbutils.fs.put(f"{TEST_S3_SOURCE_PATH}/{file_name}", csv_content.strip(), overwrite=True)
    
    # 3. 本番と同じAuto Loaderパイプラインを実行して、テスト用テーブルにデータを書き込む
    raw_stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", TEST_SCHEMA_LOCATION)
        .option("header", "true")
        .load(TEST_S3_SOURCE_PATH)
    )
    
    bronze_processed_df = s3_to_bronze(raw_stream_df)
    
    query = (
        bronze_processed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{TEST_SCHEMA_LOCATION}checkpoint/")
        .trigger(availableNow=True)
        .toTable(TEST_BRZ_TABLE)
    )
    query.awaitTermination()
    
    yield # 各テスト関数（test_st_brz_xxx）に処理を渡す
    
    # 4. 後片付け：テスト終了後に環境を掃除
    dbutils.fs.rm(TEST_S3_SOURCE_PATH, recurse=True)
    dbutils.fs.rm(TEST_SCHEMA_LOCATION, recurse=True)

# =====================================================================
# 2. 各テストケースの記述（関数を明確に分割）
# =====================================================================

def test_st_brz_001():
    """ST-BRZ-001: ファイル検知・正常ファイル読み込み疎通の検証"""
    # テスト用テーブルからデータをロード
    result_df = spark.read.table(TEST_BRZ_TABLE)
    
    # 【アサーション】Auto Loaderがファイルを自動検知し、全件（2件）漏れなく処理できているか
    assert result_df.count() == 2


def test_st_brz_002():
    """ST-BRZ-002: テーブル格納（初期ロード時のスキーマ生成、データ無加工格納、メタデータ付与）の検証"""
    # テスト用テーブルからデータをロード
    result_df = spark.read.table(TEST_BRZ_TABLE)
    
    # 特定のレコードを抽出して詳細検証
    sample_row = result_df.filter(F.col("log_id") == "F03-L0001").collect()[0]
    
    # ① スキーマが正しく生成され、業務データが「文字列型の無加工」で格納されているか
    assert sample_row["vin"] == "VIN-F03-00001"
    assert sample_row["factory_id"] == "F03"
    assert sample_row["cycle_time_sec"] == "60.0" # キャストされずString型であること
    
    # ② メタデータカラム _input_file_path が存在し、配置したCSVファイル名を指しているか
    assert "_input_file_path" in result_df.columns
    assert "test_source_vehicle_logs.csv" in sample_row["_input_file_path"]
    
    # ③ メタデータカラム _processed_timestamp が存在し、データ型がTimestamp型か
    assert "_processed_timestamp" in result_df.columns
    assert isinstance(sample_row["_processed_timestamp"], datetime)

# =====================================================================
# 3. Databricks上でPytestを実行するためのトリガー
# =====================================================================
import sys
exit_code = pytest.main(["-v", "-s", "__file__"])
assert exit_code == 0, "Pytest failed!"