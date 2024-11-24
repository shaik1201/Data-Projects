# Data Engineering Project: Data Pipeline and Visualization

This project demonstrates the creation of a scalable data pipeline using modern data engineering tools and technologies. The workflow involves extracting raw data, processing it for analytics, and creating insightful visualizations.

## Tools and Technologies Used
- **Apache Airflow**: Workflow orchestration and scheduling.
- **Apache Spark**: Distributed data processing and transformation.
- **Google Cloud Storage (GCS)**: Staging raw and processed data.
- **Google BigQuery**: Data warehousing for analytics.
- **Power BI**: Interactive data visualization and reporting.

---

## Project Overview

### Objective
The objective of this project is to build an end-to-end data pipeline to:
1. Extract raw data from a source.
2. Transform the data into a clean and usable format.
3. Store the processed data in a data warehouse.
4. Analyze the data using visualizations to derive business insights.

### Workflow Diagram

```plaintext
Source Data -> Extract (Airflow) -> Transform (Spark) -> Load to GCS -> BigQuery -> Power BI
```


# Implementation Details
1. Data Extraction
Tool: Apache Airflow
Description: Airflow orchestrates the data extraction process. DAGs (Directed Acyclic Graphs) are configured to fetch the raw data periodically from the source (e.g., APIs, flat files).
2. Data Transformation
Tool: Apache Spark
Description: Spark processes and cleanses the raw data, handles missing values, and performs aggregations or feature engineering as required.
Storage: The processed data is saved to Google Cloud Storage in a structured format (e.g., Parquet, CSV).
3. Data Loading
Tool: Google BigQuery
Description: Processed data is loaded into BigQuery for analytical querying. BigQuery's scalability ensures efficient handling of large datasets.
4. Visualization and Analysis
Tool: Power BI
Description: Interactive dashboards and reports are built to analyze key metrics and derive insights from the processed data stored in BigQuery.

# Project Architecture

