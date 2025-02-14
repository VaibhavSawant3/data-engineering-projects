# Azure Data Engineering Project 🚀

This project showcases an **end-to-end data pipeline** using **Azure Data Factory, Azure Data Lake Gen 2, SQL Server, and Databricks** to process and transform data from **GitHub to a structured data warehouse**.

## 📌 Project Overview

The goal of this project is to **ingest, transform, and store data** using a multi-layered architecture (**Bronze, Silver, and Gold**) while implementing **incremental data loading**.

## 🏗️ Architecture & Workflow

### **1️⃣ Data Ingestion (Azure Data Factory)**
- **CopyGitData Activity**: Copies source data from a **GitHub repository** to **SQLDB** (`source_cars_data` table).
- **Lookup Activities**:
  - **Last Load Lookup**: Retrieves the last update date.
  - **Current Load Lookup**: Fetches the maximum date of the current dataset.
- **Incremental Data Load**: Uses lookup values to copy **only new or updated records** to `source_cars_data`.
- **Stored Procedure Activity**: Updates the last load date in **Last Load Lookup**.

### **2️⃣ Data Processing & Transformation (Azure Databricks)**
- **Created Catalogs & External Locations** for **Bronze, Silver, and Gold** containers in **Azure Data Lake**.
- **Bronze Layer**: Raw data copied from SQLDB.
- **Silver Layer**: Transformation applied via **Silver Layer Notebook**, storing data as a **single table** in the Silver container.
- **Gold Layer**:
  - **Dimension Table Notebooks**: Creates structured **dimension tables**.
  - **Fact Table Notebook**: Builds a **fact table** for analytical queries.
- **Data Factory Notebook Activity**: Orchestrates Databricks notebooks in **Data Factory**.

## 🔧 Technologies Used

- **Azure Data Factory** (ETL Orchestration)
- **Azure Data Lake Gen 2** (Storage)
- **Azure SQL Server** (Database)
- **Azure Databricks** (Processing & Transformation)
- **PySpark & SQL** (Data Manipulation)

## 🚀 How to Use This Project

1. Clone the repository:
   ```sh
   git clone https://github.com/VaibhavSawant3/data-engineering-project.git
2. Navigate to the project directory and follow the instructions in the respective notebooks and pipeline configurations.
3. Deploy Azure Data Factory pipeline and run the notebooks to process data.

## 🔮 **Future Enhancement**

- Automate data ingestion with event triggers.
- Implement real-time data streaming using Azure Event Hubs.
- Optimize performance with Delta Lake and partitioning.

🌟 **If you find this project helpful, don't forget to star the repository!** 🌟
