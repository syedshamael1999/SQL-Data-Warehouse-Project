# Data Warehouse and Analytics Project
This project demonstrates the development of a Data Warehouse using MS SQL Server, incorporating ETL processes, data modeling, and data analytics. This project also highlights industry best practices in data engineering and analytics.  

---
## 🏛️ Data Architecture
This project’s data architecture is built on the Medallion Architecture, utilizing Bronze, Silver, and Gold layers: 
- 🥉 **Bronze Layer**: Stores raw unprocessed data from source systems. Data is ingested from CSV files into the SQL Server database.
- 🥈 **Silver Layer**: Includes data cleansing, standardization, and normalization processes to prepare data for analysis.
- 🥇 **Gold Layer**: Houses business-ready data modeled into a Star Schema, optimized for reporting and analytics.

(The architecture was designed using Draw.io for visualization and documentation.) 

![image](https://github.com/user-attachments/assets/a5152e15-8fa1-4b4a-bfd7-9e4a1d29895e)

---
## 📌 Project Overview
Key Components:
1. **Data Architecture**: Designed using the Medallion Architecture (Bronze, Silver, and Gold layers) to ensure structured data refinement.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.
<br><br> 

💡 This project showcases my expertise in:
- SQL Development
- Data Architecture
- Data Engineering
- ETL Pipeline Development
- Data Modeling
- Data Analytics
---
## 📋 Project Requirements
### 1. Building the Data Warehouse (Data Engineering)  

#### Objective  
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.  

#### Specifications  
- **Data Sources:** Import data from two source systems (ERP and CRM) provided as CSV files.  
- **Data Quality:** Cleanse and resolve data quality issues prior to analysis.  
- **Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries.  
- **Scope:** Focus on the latest dataset only; historization of data is not required.  
- **Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics teams.  
