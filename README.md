# Toronto Transit & Weather Resilience Pipeline

### **End-to-End Data Engineering via Medallion Architecture in Microsoft Fabric**

This project analyzes the impact of weather (precipitation and temperature) on Toronto Transit Commission (TTC) service efficiency. By automating the ingestion of transit and meteorological data, the pipeline quantifies how urban mobility is affected by environmental conditions.


## Architecture: The Medallion Pattern
The project implements a **Medallion Architecture** to ensure data quality and lineage as it moves from raw API calls to analytical insights.

* **Bronze (Raw):** Ingests raw JSON (Weather) and CSV (TTC) data via Python `requests` directly into the Lakehouse `Files` section.
* **Silver (Cleaned):** PySpark processing to enforce schemas, standardize column names, and flatten nested JSON structures using `arrays_zip` and `explode`.
* **Gold (Curated):** Hourly aggregations and a complex outer-join between Bus, Subway, and Weather datasets. Uses `F.coalesce` to maintain a unified temporal index.


## Tech Stack & Techniques
* **Orchestration:** **Fabric Data Factory Pipelines** for automated sequential notebook execution.
* **Data Processing:** **PySpark (Spark SQL)** for scalable ETL and complex data transformations.
* **Storage:** **Delta Lake** tables for ACID-compliant storage and schema evolution.
* **Analytics:** **Power BI (Direct Lake)** for real-time visualization of Delta tables without data movement.
* **Data Sources:** [City of Toronto Open Data](https://open.toronto.ca/) & [Open-Meteo API](https://open-meteo.com/).


## Key Insights from Analysis
* **The Rain-Delay Correlation:** Trend analysis confirms a positive correlation between precipitation volume (mm) and total bus delay minutes.
* **The Freezing Threshold:** Significant delay spikes correlate with temperatures between **-5°C and 5°C**, indicating mechanical and operational vulnerability during freeze-thaw cycles.
* **Resilience Gap:** While both systems are affected, surface-level bus routes show significantly higher sensitivity to heavy rain events compared to the subway system.


## Deployment & Usage

### **Prerequisites**
* An active **Microsoft Fabric** workspace.

### **Execution Steps**
1.  **Sync to Fabric:** Use the **Source Control** feature in your Fabric Workspace to connect and sync this GitHub repository.
2.  **Run the Pipeline:** Locate the **`pipeline`** item in your workspace and click **Run**. 
    * *The pipeline automatically orchestrates the execution of: `Fetch_Data` → `Silver_Process` → `Aggregate_Hourly` → `Gold_Process`.*
3.  **Visualize:** Once the pipeline completes, open the `TTC_Weather_Report` to explore the interactive dashboard.

---
*Developed as a demonstration of modern Data Engineering practices in Microsoft Fabric.*
