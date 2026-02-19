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

def aggregate_data_hourly(table_name, saved_table_name):
    # 1. Load your data from lakehouse
    df = spark.read.table(table_name)

    # 2. Filter, Select, and Aggregate to Hourly
    hourly_ttc_delays = (df
        .select("datetime", "min_delay")
        .filter(F.col("min_delay") > 0)
        .withColumn("hourly_timestamp", F.date_trunc("hour", F.col("datetime")))
        .groupBy("hourly_timestamp")
        .agg(
            F.sum("min_delay").alias("total_delay_mins"),
            F.avg("min_delay").alias("avg_delay_mins"),
            F.count("*").alias("incident_count") # Added to see how many delays happened per hour
        )
        .sort("hourly_timestamp")
    )

    # 3. Save table in delta format
    hourly_ttc_delays.write.format("delta").mode("overwrite").saveAsTable(saved_table_name)

aggregate_data_hourly("silver_ttc_bus_delays", "ttc_bus_delays_hourly")
aggregate_data_hourly("silver_ttc_subway_delays", "ttc_subway_delays_hourly")

print("Successfully Aggregate TTC Data to Hourly!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
