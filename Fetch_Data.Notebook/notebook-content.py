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

# Import libraries
import requests
import json
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetching the entire year of 2025 for Toronto
weather_2025_url = "https://archive-api.open-meteo.com/v1/archive?latitude=43.65&longitude=-79.38&start_date=2025-01-01&end_date=2025-12-31&hourly=temperature_2m,precipitation,rain,snowfall&timezone=America/Toronto"

print("Fetching full year 2025 weather data...")
response = requests.get(weather_2025_url)

if response.status_code == 200:
    path = "/lakehouse/default/Files/raw_weather_toronto_2025.json"
    with open(path, "w") as f:
        json.dump(response.json(), f)
    print(f"✅ Full Year Weather saved to: {path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def download_toronto_data(package_id, year_filter, file_name_prefix):
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    package_url = f"{base_url}/api/3/action/package_show"
    
    # 1. Get metadata
    package = requests.get(package_url, params={"id": package_id}).json()
    
    for resource in package["result"]["resources"]:
        # Match the year (e.g., '2025')
        if year_filter in resource["name"]:
            file_url = resource["url"]
            
            # Determine extension: If it's a datastore dump, default to csv
            if "datastore/dump" in file_url:
                file_extension = "csv"
            else:
                file_extension = file_url.split('.')[-1].lower()
            
            print(f"Attempting to download: {resource['name']}")
            res_data = requests.get(file_url)
            
            if res_data.status_code == 200:
                # FIXED PATH: Using the relative path to the Lakehouse
                save_path = f"Files/raw_{file_name_prefix}_2025.{file_extension}"
                
                # In Fabric, we can write directly to the local mount
                full_path = f"/lakehouse/default/{save_path}"
                
                with open(full_path, "wb") as f:
                    f.write(res_data.content)
                print(f"✅ Successfully saved to: {full_path}")
                return 
            else:
                print(f"❌ Failed to download from {file_url}")

# Execute
download_toronto_data("ttc-bus-delay-data", "2025", "ttc_bus")
download_toronto_data("ttc-subway-delay-data", "2025", "ttc_subway")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
