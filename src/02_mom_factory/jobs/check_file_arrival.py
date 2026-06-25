from datetime import datetime, timezone, timedelta
import pytz
import sys

mom_catalog_name = sys.argv[1]

brz_table_name = f"{mom_catalog_name}.bronze.brz_mom_factory"

df = spark.sql(f"""
    SELECT MAX(to_timestamp(timestamp, 'yyyy/M/d HH:mm')) AS max_time 
    FROM {brz_table_name}
""")

latest_time = df.collect()[0]["max_time"]


jst = pytz.timezone("Asia/Tokyo")
now = datetime.now(jst)

if latest_time.tzinfo is None:
    latest_time = jst.localize(latest_time)

threshold = now - timedelta(hours=6)

if latest_time < threshold:
    error_msg = f"【警告】ブロンズテーブルの最新データ時刻（{latest_time}）が、現在時刻より6時間以上乖離しています。"

    raise RuntimeError(error_msg)
