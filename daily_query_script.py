"""
The querying script to be ran every day
"""

# Imports
print("Importing libraries...")
from pathlib import Path
import datetime
import sqlite3
from pyspark.sql import types as sparktypes
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
print("Imports complete")

# Constants (settings, paths etc)
DB_PATH = ( # macOS path to SQLite database file. Feel free to relocate
    Path("~") / "Library" / "Group Containers" /
    "group.net.whatsapp.WhatsApp.shared" /
    "ChatStorage.sqlite"
).expanduser()
PHONE_NAME_MAP = {
    "62123456789": "rel01",
    "621357986420": "rel02"
}
SAVE_FOLDER_PATH = (
    Path(__file__).parent / "parquet_data"
)

# Script

# 1. Connect to database

print("Connecting to database...")
if (not DB_PATH.is_file()):
    print("Error: database file not found at")
    print(DB_PATH)
    print("which is the path specified in the daily script.")

conn = sqlite3.connect(DB_PATH)
print("Database connected")

# 2. Deal with timestamps

dt_unix_epoch = datetime.datetime(
    1970, 1, 1, 0, 0, 0,
    tzinfo=datetime.timezone.utc
)
timestamp_unix_epoch = dt_unix_epoch.timestamp()
timestamp_unix_epoch_int = int(timestamp_unix_epoch)
dt_wa_epoch = datetime.datetime(
    2001, 1, 1, 0, 0, 0,
    tzinfo=datetime.timezone.utc
)
timestamp_wa_epoch = dt_wa_epoch.timestamp()
timestamp_wa_epoch_int = int(timestamp_wa_epoch)

def wa_timestamp_from_unix(unix_timestamp):
    return (
        unix_timestamp -
        (timestamp_wa_epoch_int - timestamp_unix_epoch_int)
    )

def unix_timestamp_from_wa(wa_timestamp):
    return (
        wa_timestamp +
        timestamp_wa_epoch_int - timestamp_unix_epoch_int
    )

date_today = (
    datetime.date.today()
    + datetime.timedelta(days=-7) # temporary shift
)
time_midnight = datetime.time(0,0,0)
dt_today_midnight = datetime.datetime.combine(date_today, time_midnight)
unix_timestamp_today = dt_today_midnight.timestamp()
wa_timestamp_today = wa_timestamp_from_unix(unix_timestamp_today)
wa_timestamp_today_int = int(wa_timestamp_today)

#date_tomorrow = date_today + datetime.timedelta(days=1)
#dt_tomorrow_midnight = datetime.datetime.combine(date_tomorrow, time_midnight)
dt_tomorrow_midnight = dt_today_midnight + datetime.timedelta(days=1)
unix_timestamp_tomorrow = dt_tomorrow_midnight.timestamp()
wa_timestamp_tomorrow = wa_timestamp_from_unix(unix_timestamp_tomorrow)
wa_timestamp_tomorrow_int = int(wa_timestamp_tomorrow)

# 3. Query data

print("Querying database...")
phone_numbers_list = PHONE_NAME_MAP.keys()
phone_rows_map = dict()
for phone_str in phone_numbers_list:
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
    ZMESSAGEDATE >= {wa_timestamp_today_int}
    AND
    ZSENTDATE < {wa_timestamp_tomorrow_int}
ORDER BY ZMESSAGEDATE ASC
LIMIT 5
;
    """)
    rows_list = cur.fetchall()
    phone_rows_map[phone_str] = rows_list

print("Querying complete. Closing database connection...")
conn.close()
print("Database connection closed")

# 4. Create PySpark dataframe, initial transforms

wa_rows_schema = sparktypes.StructType([
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
        dataType=sparktypes.DoubleType(), nullable=False
    )
])

# probably not needed since it's just addition/subtraction but just in case
udf_unix_timestamp_from_wa = sf.udf(
    f=unix_timestamp_from_wa,
    returnType=sparktypes.IntegerType()
)

print("Starting SparkSession...")
spark = (
    SparkSession.builder
    .appName("DailyQuery")
    .getOrCreate()
)
print("SparkSession started")

phone_df_map = dict()
print("Creating PySpark DataFrames...")
for phone_number, rows_list in phone_rows_map.items():
    current_df = spark.createDataFrame(
        data=rows_list, schema=wa_rows_schema
    ).withColumns({
        "message_timestamp":
        (
            unix_timestamp_from_wa(sf.col("message_timestamp"))
            .cast("timestamp")
        ),
        "sent_timestamp":
        (
            unix_timestamp_from_wa(sf.col("sent_timestamp"))
            .cast("timestamp")
        )
    })
    phone_df_map[phone_number] = current_df
print("DataFrames created")

# debug
print("Displaying DataFrames...")
for phone_number, cur_df in phone_df_map.items():
    print("Phone number:", phone_number)
    cur_df.show()
print("DataFrames displayed")

# 5. Save "raw" data to Parquet

cur_year = str(date_today.year)
cur_month = str(date_today.month).zfill(2)
cur_date_day = str(date_today.day).zfill(2)
specific_folder = (
    SAVE_FOLDER_PATH / cur_year / cur_month / cur_date_day
)
print("Saving DataFrames at:", specific_folder)
specific_folder.mkdir(parents=True, exist_ok=True)
for phone_number, cur_df in phone_df_map.items():
    phone_name = PHONE_NAME_MAP[phone_number]
    filename = f"{cur_year}_{cur_month}_{cur_date_day}_query_{phone_name}"
    full_file_path_ext = (specific_folder / filename).with_suffix(".parquet")

    #cur_df.write.parquet(full_file_path_ext)
    # AttributeError: 'PosixPath' object has no attribute '_get_object_id'

    cur_df.write.parquet(str(full_file_path_ext))
print("DataFrames saved")
