# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c97d3bb4-0a37-4f27-935b-848457186ec8",
# META       "default_lakehouse_name": "lh_Toronto_Transit_Weather",
# META       "default_lakehouse_workspace_id": "cfe80c57-ed9a-484e-a3d9-7589e3a670b8",
# META       "known_lakehouses": [
# META         {
# META           "id": "c97d3bb4-0a37-4f27-935b-848457186ec8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Read the Raw Weather JSON Data
df_weather_raw = spark.read.option("multiline", "true").json("Files/raw_weather_toronto_2025.json")

# 2. Explode the hourly arrays into rows
df_weather_hourly = df_weather_raw.select(
    F.explode(F.arrays_zip(
        F.col("hourly.time"), 
        F.col("hourly.temperature_2m"), 
        F.col("hourly.precipitation")
    )).alias("weather_row")
).select(
    F.col("weather_row.time").alias("datetime_str"),
    F.col("weather_row.temperature_2m").alias("temp_c"),
    F.col("weather_row.precipitation").alias("precip_mm")
)

# 3. Convert the string column to a real Timestamp
df_weather_silver = df_weather_hourly.withColumn(
    "datetime", 
    F.to_timestamp(F.col("datetime_str"), "yyyy-MM-dd'T'HH:mm")
).select("datetime", "temp_c", "precip_mm")

# 4. Save as a Silver Table
df_weather_silver.write.format("delta").mode("overwrite").saveAsTable("silver_toronto_weather")

print("Weather Data converted!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def convert_ttc_csv(csv_path, table_name):
    # 1. Read the Raw TTC Data 
    df = spark.read.format("csv").option("header", "true").load(csv_path)

    # 2. Standardize column names
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower().replace(" ", "_"))

    # 3. Fix the datetime
    df_cleaned = df.withColumn(
        "datetime", 
        F.to_timestamp(
            F.concat(
                F.split(F.col("date"), "T")[0],
                F.lit(" "), 
                F.col("time")
            ), 
            "yyyy-MM-dd HH:mm"
        )
    )

    # 4. Clean up types for other columns
    df_cleaned = df_cleaned.withColumn("min_delay", F.col("min_delay").cast("int")) \
                                .withColumn("min_gap", F.col("min_gap").cast("int"))

    # 5. Drop old columns
    df_cleaned = df_cleaned.drop("date", "time")

    # 6. Save table in delta format
    df_cleaned.write.format("delta").mode("overwrite").saveAsTable(table_name)

convert_ttc_csv("Files/raw_ttc_bus_2025.csv", "silver_ttc_bus_delays")
convert_ttc_csv("Files/raw_ttc_subway_2025.csv", "silver_ttc_subway_delays")

print("TTC Delay Data converted!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
