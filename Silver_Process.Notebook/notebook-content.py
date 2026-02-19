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

# 1. Read the JSON
df_weather_raw = spark.read.option("multiline", "true").json("Files/raw_weather_toronto_2025.json")

# 2. "Explode" the hourly arrays into rows
# Note: Spark now preserves the names 'time', 'temperature_2m', and 'precipitation' inside the zip
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
).select("datetime", "temp_c", "precip_mm") # Keep only the clean columns

# 4. Save as a Silver Table
df_weather_silver.write.format("delta").mode("overwrite").saveAsTable("silver_toronto_weather")

print("✅ Weather Data converted. The 'datetime' column is now a formal Timestamp!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Read the Raw TTC Bus Data 
df_bus = spark.read.format("csv").option("header", "true").load("Files/raw_ttc_bus_2025.csv")

# 2. Standardize column names
for col in df_bus.columns:
    df_bus = df_bus.withColumnRenamed(col, col.lower().replace(" ", "_"))

# 3. FIX THE DATETIME
df_bus_cleaned = df_bus.withColumn(
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
df_bus_cleaned = df_bus_cleaned.withColumn("min_delay", F.col("min_delay").cast("int")) \
                               .withColumn("min_gap", F.col("min_gap").cast("int"))

# 5. Drop old columns
df_bus_cleaned = df_bus_cleaned.drop("date", "time")

# 6. Save with overwriteSchema
df_bus_cleaned.write.format("delta").mode("overwrite").saveAsTable("silver_ttc_bus_delays")

print("✅ Success! Time is no longer haunted by current dates.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Read the Raw TTC Subway Data 
df_subway = spark.read.format("csv").option("header", "true").load("Files/raw_ttc_subway_2025.csv")

# 2. Standardize column names
for col in df_subway.columns:
    df_subway = df_subway.withColumnRenamed(col, col.lower().replace(" ", "_"))

# 3. FIX THE DATETIME
df_subway_cleaned = df_subway.withColumn(
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
df_subway_cleaned = df_subway_cleaned.withColumn("min_delay", F.col("min_delay").cast("int")) \
                               .withColumn("min_gap", F.col("min_gap").cast("int"))

# 5. Drop old columns
df_subway_cleaned = df_subway_cleaned.drop("date", "time")

# 6. Save with overwriteSchema
df_subway_cleaned.write.format("delta").mode("overwrite").saveAsTable("silver_ttc_subway_delays")

print("✅ Success! Time is no longer haunted by current dates.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
