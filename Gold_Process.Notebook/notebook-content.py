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

# Load the hourly tables you just created
df_bus = spark.read.table("ttc_bus_delays_hourly")
df_subway = spark.read.table("ttc_subway_delays_hourly")
df_weather = spark.read.table("silver_toronto_weather")

# 1. Join Bus and Subway on timestamp
# We use an 'outer' join in case there was a delay on the subway but not the bus (or vice versa)
ttc_combined = (df_bus.join(
    df_subway, 
    df_bus.hourly_timestamp == df_subway.hourly_timestamp, 
    "outer"
)
.select(
    F.coalesce(df_bus.hourly_timestamp, df_subway.hourly_timestamp).alias("hourly_timestamp"),
    df_bus["total_delay_mins"].alias("bus_total_delay"),
    df_bus["avg_delay_mins"].alias("bus_avg_delay"),
    df_subway["total_delay_mins"].alias("subway_total_delay"), # Note: Rename these clearly
    df_subway["avg_delay_mins"].alias("subway_avg_delay")
))

# 2. Join with Weather
gold_analysis = (ttc_combined.join(
    df_weather, 
    ttc_combined.hourly_timestamp == df_weather.datetime, 
    "inner"
)
.select(
    "datetime",
    "bus_total_delay",
    "bus_avg_delay",
    "subway_total_delay",
    "subway_avg_delay",
    "temp_c",
    "precip_mm"
).fillna(0)) # Fill nulls with 0 for hours with no delays

# 3. Save as the final Gold Table
gold_analysis.write.format("delta").mode("overwrite").saveAsTable("gold_ttc_weather_impact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
