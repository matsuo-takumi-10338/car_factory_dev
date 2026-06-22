import sys

sys.path.insert(0, sys.argv[2])

import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ジョブの引数からカタログ名とワークスペースパスを取得
catalog_name = sys.argv[1]
dbutils = spark.getOrCreate()._jvm.com.databricks.service.DBUtils

# アンロード対象のテーブルと、出力したい固定ファイル名のマッピング
target_tables = {
    "gld_bi_mom_factory": "bi_mom_factory.csv",
    "gld_api_vehicle_trace": "api_vehicle_trace.csv",
    "gld_api_operator_status": "api_operator_status.csv",
    "gld_api_facility_maintenance": "api_facility_maintenance.csv",
}

# 最終的なS3の出力先ルート（環境に合わせて変更してください）
# ※例として、バケット下の「unload」ディレクトリに出力
final_s3_base_dir = f"s3://cf-mom-factory-bucket-dev/mom_factory_dev/unload/"


def unload_table_to_single_csv(table_name, target_file):
    full_table_name = f"`{catalog_name}`.`gold`.`{table_name}`"
    final_s3_dir = f"{final_s3_base_dir}{table_name}/"
    temp_s3_dir = f"{final_s3_dir}temp/"

    print(f"🔄 アンロード開始: {full_table_name} -> {final_s3_dir}{target_file}")

    # 1. 対象データを読み込む
    df = spark.read.table(full_table_name)

    # 2. データを1つにまとめて一時ディレクトリにCSV出力
    (
        df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(temp_s3_dir)
    )

    # 3. 拡張子が「part-」で始まる実際のCSVファイルを探す
    temp_files = dbutils.fs.ls(temp_s3_dir)
    part_file_path = [f.path for f in temp_files if f.name.startswith("part-")][0]

    # 4. 指定の名前で最終目的地へ移動（リネーム）
    final_destination = f"{final_s3_dir}{target_file}"
    dbutils.fs.mv(part_file_path, final_destination)

    # 5. 一時ディレクトリのクリーンアップ
    dbutils.fs.rm(temp_s3_dir, recurse=True)
    print(f"✅ アンロード完了: {target_file}")


# 4つのテーブルを順次処理
for table, file_name in target_tables.items():
    try:
        unload_table_to_single_csv(table, file_name)
    except Exception as e:
        print(f"❌ {table} のアンロード中にエラーが発生しました: {e}")