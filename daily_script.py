"""
The script to be ran every day
"""

# Imports
from pathlib import Path
import datetime
import sqlite3
from pyspark.sql import types as sparktypes
from pyspark.sql import SparkSession

# Constants (settings, paths etc)
DB_PATH = ( # macOS path to SQLite database file. Feel free to relocate
    Path("~") / "Library" / "Group Containers" /
    "group.net.whatsapp.WhatsApp.shared" /
    "ChatStorage.sqlite"
)
PHONE_NUMBERS = ["628123456789"]

# Script

# 1. Connect to database

if (not DB_PATH.is_file()):
    print("Error: database not found at")
    print(DB_PATH)
    print("which is the path specified in the daily script.")

conn = sqlite3.connect(DB_PATH)

# 2. Deal with timestamps

dt_wa_epoch = datetime.datetime(
    2001, 1, 1, 0, 0, 0,
    tzinfo=datetime.timezone.utc
)
timestamp_wa_epoch = dt_wa_epoch.timestamp()

date_today = datetime.date.today()
time_midnight = datetime.time(0,0,0)
dt_today_midnight = datetime.datetime.combine(date_today, time_midnight)
raw_timestamp_today = dt_today_midnight.timestamp()
adjusted_timestamp_today = int(raw_timestamp_today - timestamp_wa_epoch)

#date_tomorrow = date_today + datetime.timedelta(days=1)
#dt_tomorrow_midnight = datetime.datetime.combine(date_tomorrow, time_midnight)
dt_tomorrow_midnight = dt_today_midnight + datetime.timedelta(days=1)
raw_timestamp_tomorrow = dt_tomorrow_midnight.timestamp()
adjusted_timestamp_tomorrow = int(raw_timestamp_tomorrow - timestamp_wa_epoch)

# 3. Query data

phone_rows_map = dict()
for phone_str in PHONE_NUMBERS:
    cur = conn.cursor()
    cur.execute(f"""
SELECT ZTEXT
    , ZMESSAGEDATE
    , ZSENTDATE
FROM ZWAMESSAGE
WHERE
    ZTEXT LIKE "https://youtu%"
    AND
    ZFROMJID = "{phone_str}@s.whatsapp.net" -- feel free to modify
    AND
    ZTOJID IS NULL
    AND
    ZMESSAGEDATE >= {adjusted_timestamp_today}
    AND
    ZSENTDATE < {adjusted_timestamp_tomorrow}
ORDER BY ZMESSAGEDATE ASC
LIMIT 5
;
    """)
    rows_list = cur.fetchall()
    phone_rows_map[phone_str] = rows_list

conn.close()

# 4. Create PySpark dataframe, initial transforms

rows_schema = sparktypes.StructType(
    [
        sparktypes.StructField(
            name="text",
            dataType=sparktypes.StringType(), nullable=False
        ),
        sparktypes.StructField(
            name="message_timestamp",
            dataType=sparktypes.IntegerType(), nullable=False
        ),
        sparktypes.StructField(
            name="sent_timestamp",
            dataType=sparktypes.IntegerType(), nullable=False
        )
    ]
)

spark = (
    SparkSession.builder
    .appName("DailyQuery")
    .getOrCreate()
)

df_list = []
for phone_number, rows_list in phone_rows_map.items():
    current_df = spark.createDataFrame(
        data=rows_list, schema=rows_schema
    )
    df_list.append(current_df)

for cur_df in df_list:
    pass # todo

# todo

#wa_epoch = datetime(2001, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

# 5. Save "raw" data to Parquet
