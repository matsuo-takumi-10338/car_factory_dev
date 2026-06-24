import glob
import pytz
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, to_date

spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")

# ==========================================
# 1. Spark処理専用関数
# ==========================================
def execute_yesterday_export(schema_path, table_name, yesterday_str, temp_dir):
    df_all = spark.table(f"{schema_path}.{table_name}")
    target_date_column = "_processed_timestamp" 
    df_delta_lazy = df_all.filter(to_date(col(target_date_column)) == lit(yesterday_str)).coalesce(1)
    
    try:
        row_count = df_delta_lazy.count()
        if row_count > 0:
            (df_delta_lazy
             .write
             .mode("overwrite")
             .option("header", "true")
             .csv(temp_dir))
        return row_count
    except Exception as e:
        raise RuntimeError(f"テーブル [{table_name}] の処理中に致命的なエラーが発生しました。理由: {e}") from e


mom_catalog_name = spark.conf.get("mom_catalog_name")
schema_path = f"{mom_catalog_name}.gold"
volume_base_path = f"/Volumes/{mom_catalog_name}/gold/"

jst_tz = pytz.timezone('Asia/Tokyo')
current_execution_time = datetime.now(jst_tz)
timestamp = current_execution_time.strftime("%Y%m%d_%H%M%S")

yesterday = current_execution_time - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

tables_df = spark.sql("SHOW TABLES IN " + schema_path)
table_list = [row.tableName for row in tables_df.collect() if not row.isTemporary]

# ==========================================
# 2. ループ処理による一括CSV出力（前日分・日次抽出）
# ==========================================
for table_name in table_list:
    
    table_basename = re.sub(r"^gld_", "", table_name)
    target_file_name = f"{mom_catalog_name}_{table_basename}_{timestamp}.csv"

    table_output_dir = f"{volume_base_path}vol_{table_basename}"
    temp_dir = f"{table_output_dir}/temp_{table_name}"
    
    row_count = execute_yesterday_export(schema_path, table_name, yesterday_str, temp_dir)
    if row_count == 0:
        dbutils.fs.rm(temp_dir, recurse=True)
        continue
        
    try:
        csv_files = glob.glob(f"{temp_dir}/part-*.csv")
        if csv_files:
            spark_generated_csv = csv_files[0]
            final_file_path = f"{table_output_dir}/{target_file_name}"
            
            dbutils.fs.mv(spark_generated_csv, final_file_path)
            
    except Exception as e:
        raise RuntimeError(f"テーブル [{table_name}] のファイル移動中にエラーが発生しました。理由: {e}") from e
        
    finally:
        dbutils.fs.rm(temp_dir, recurse=True)
