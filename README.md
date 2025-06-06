# Data Warehouse Project (Data Engineering)
This project demonstrates how I developed a Data Warehouse (Medallion Architecture) using MS SQL Server, incorporating ETL processes, data modeling, and data analytics. This project also highlights industry best practices in data engineering and analytics.  

The ETL logic was built using T-SQL stored procedures and scripts, following standards for staging, cleansing, and dimensional modeling. This logic is portable, and can be migrated to cloud-native ETL platforms such as Databricks SQL or dbt with minimal refactoring.

[Project Roadmap (Epics & Tasks)](https://www.notion.so/Data-Warehouse-Project-1c1ede7b016a80c9bee9faa1763beaec?pvs=4)

---
## 🏛️ Data Architecture
This project’s data architecture is built on the Medallion Architecture, utilizing Bronze, Silver, and Gold layers: 
- 🥉 **Bronze Layer**: Stores raw unprocessed data from source systems. Data is ingested from CSV files into the SQL Server database.
- 🥈 **Silver Layer**: Includes data cleansing, standardization, and normalization processes to prepare data for analysis.
- 🥇 **Gold Layer**: Houses business-ready data modeled into a Star Schema, optimized for reporting and analytics.

*(The architecture was designed using Draw.io for visualization and documentation.)* 

![image](https://github.com/user-attachments/assets/b4b2cf6b-160e-441a-8c33-53f95e9c7225)


---
## 📌 Project Overview
Key Components:
1. **Data Architecture**: Designed using the Medallion Architecture (Bronze, Silver, and Gold layers) to ensure structured data refinement.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.
<br><br> 

💡 This project showcases my expertise in:
- Project Management
- SQL Development
- Data Architecture
- Data Engineering
- ETL Pipeline Development
- Data Modeling
- Data Analytics

---
## 👀 Project Walkthrough

> 📌 For a quick step-by-step walkthrough of the entire project, check out the  
> 👉 [**Project Walkthrough**](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/docs/project_walkthrough.md)
---
[**Project Documents**](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/tree/main/docs) | [**Layer Scripts - DDL & Stored Procedures**](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/tree/main/scripts) | [**Quality Check Scripts**](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/tree/main/tests) | [**Source Systems**](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/tree/main/datasets)




