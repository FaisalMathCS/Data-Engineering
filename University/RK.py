import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from platform import python_version


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, decode
import json
from pyspark.sql import functions as F
from pyspark.sql import Row


print("STarting a spark Session")
spark = SparkSession.builder \
    .appName("ConsumeKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()
print("Session Established!")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


df_raw = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input") \
    .load()
#df will be retrned as binary so needs parsing
df = df_raw.select(decode(col("value"), "UTF-8").alias("value_string"))

#df will be in json format so convert to df using the structure of the file
def parse_and_cast(json_str):
    # Parse the JSON string
    data = json.loads(json_str)

    # Manually cast the fields
    return Row(
        _c0=str(data["_c0"]),
        UTC=str(data["UTC"]),
        Temperature_C=float(data["Temperature[C]"]),
        Humidity_Percent=float(data["Humidity[%]"]),
        TVOC_ppb=int(data["TVOC[ppb]"]),
        eCO2_ppm=int(data["eCO2[ppm]"]),
        Raw_H2=int(data["Raw H2"]),
        Raw_Ethanol=int(data["Raw Ethanol"]),
        Pressure_hPa=float(data["Pressure[hPa]"]),
        PM1=float(data["PM1"]),
        PM2_5=float(data["PM2_5"]),
        NC0_5=int(data["NC0_5"]),
        NC1_5=float(data["NC1_5"]),
        NC2_5=float(data["NC2_5"]),
        CNT=int(data["CNT"]),
        Fire_Alarm=str(data["Fire Alarm"])
    )

# Apply the function to each row
# rdd = df.rdd.map(lambda row: parse_and_cast(row.value_string))

# Convert the RDD back to a DataFrame
# new_df = spark.createDataFrame(rdd)

# Show the result
# new_df.show(truncate=False)


rdd = df.rdd.map(lambda row: parse_and_cast(row.value_string))

new_df = spark.createDataFrame(rdd)

new_df.show(truncate = False)

aggregated_df = new_df.groupBy("Fire_Alarm").agg(
    F.max("Temperature_C").alias("Max_Temperature"),
    F.min("Temperature_C").alias("Min_Temperature"),
    F.avg("Humidity_Percent").alias("Avg_Humidity"),
)
aggregated_df.show()

spark.stop()
