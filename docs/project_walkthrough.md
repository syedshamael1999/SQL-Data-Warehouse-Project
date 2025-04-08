# Data Warehouse Project Walkthrough

This project demonstrates how I developed a Data Warehouse (Medallion Architecture) using MS SQL Server, incorporating ETL processes, data modeling, and data analytics. This project also highlights industry best practices in data engineering and analytics.

- [Project Requirements](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/docs/project_requirements.md)
- [Naming Convention](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/docs/naming_convention.md) - *Guidelines for naming everything in the project*
- [Source Systems](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/tree/main/datasets) - *Data Source for this project*
- [Project Roadmap (Epics & Tasks)](https://www.notion.so/Data-Warehouse-Project-1c1ede7b016a80c9bee9faa1763beaec?pvs=4)
---

### 🧠 Data Warehouse Approach Selection

The following diagram shows the different arhitecture options for the data warehouse. *The approach chosen was the **Medallion Architecture***  

![image](https://github.com/user-attachments/assets/0c97bfb0-0f43-4339-8b22-6109e258ad72)

*The **Medallion Architecture** is a layered data design pattern that organizes data into* 🥉 *Bronze (raw),* 🥈 *Silver (cleaned), and*🥇 *Gold (business-ready) stages to ensure quality, scalability, and reliable analytics.*

---
### 🏛️ Data Architecture

This diagram shows the data flow from source systems (CRM & ERP) into the data warehouse using the Medallion Architecture, where raw CSV files move through the ETL process across the Bronze, Silver, and Gold layers to produce analytics-ready data.  

![image](https://github.com/user-attachments/assets/6b34206f-19b4-4e56-96b9-a309ee212b6f)

---

### 📑 Medallion Architecture Specifications

![image](https://github.com/user-attachments/assets/b77d738e-cb06-45fb-8cc1-608d169c75bc)  

---
### 🗄️ Creating Database and schemas

**The first step** is to create a database in Microsoft SQL Server Management Studio (SSMS). The following script creates a new database named `DataWarehouse` after checking if it already exists. If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
within the database: `bronze`, `silver`, and `gold`. 

🔹 [Database & Schemas Script](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/init_database.sql)


### 🔍 Verifying the Creation

To confirm the successful creation of the database and its schemas after the script is executed:

**1. Open Object Explorer**  
**2. Navigate to:** `Object Explorer → Databases → DataWarehouse → Security → Schemas`

You should see the following user-defined schemas: `bronze`, `silver`, `gold`

These schemas represent the three layers of the **Medallion Architecture** and will store the respective tables as you move through the ETL pipeline.

---

### 🔄 ETL (Extraction, Transformation, Load)

This image provides an overview of the ETL process, illustrating key methods and techniques used in data extraction, transformation, and loading. The highlighted components in the image below are the ones implemented in this project:

![image](https://github.com/user-attachments/assets/03755803-1c9e-4cd1-a641-d4d2c1f0791c)  

---
##  🥉 Bronze Layer (Raw Data)

*The Bronze Layer is the raw data layer where unprocessed data is ingested from various sources (CRM & ERP). It captures data in its original format, preserving its fidelity for auditing, tracing, or reprocessing needs.*

Steps followed during the Bronze Layer development: 

![image](https://github.com/user-attachments/assets/f0f5a534-33da-4963-b05a-15e020322df0)  

Set up a meeting with source system experts to gather insights about the data. This step is essential for designing accurate extraction scripts and avoiding mistakes in the pipeline.

![image](https://github.com/user-attachments/assets/85877495-4a59-488c-8d3c-a9ffd0d92004)

### 🛠️ Coding and Validating

[DDL Script (Bronze Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/bronze/ddl_bronze.sql) - *Create bronze tables* 

**Verify Creation**: `Object Explorer → Databases → Datawarehouse → Tables`

![image](https://github.com/user-attachments/assets/1cebe315-50e8-42f5-92dd-c633ede07733)


[Stored Procedure (Bronze Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/bronze/proc_load_bronze.sql) - *Loads data from external CSV files into the bronze layer* (`BULK INSERT`)

**Verify Creation**: `Object Explorer → Databases → Datawarehouse → Programmability → Stored Procedures`

![image](https://github.com/user-attachments/assets/700dff58-3eb0-4aee-956e-9d326d54ddf6)

***Usage**: Running the following code executes the `bronze.load_bronze` stored procedure (It extracts raw data from the source -> loads that raw data into tables under the `bronze` schema).*
```sql 
EXEC bronze.load_bronze; 
```

After Loading data, do the following quality checks:
1. Check if data was added with a simple SELECT statement.
2. Check Row count.
3. Check that the data has not shifted and is present in the correct columns.

   - Shifts are common when loading CSV files due to reasons such as: (a) Field separater is wrong (comma). (b) separator is present in the values and SQL isnt able to split data properly.

---
## 🥈 Silver Layer (Cleaned Data)

*The Silver Layer is the cleansed and transformed data layer, where raw data from the Bronze Layer is filtered, deduplicated, and enriched to ensure consistency and quality—making it ready for business logic and analysis.*

Steps followed during the Silver Layer development:  

![image](https://github.com/user-attachments/assets/0b0f0302-f4d9-4a30-b09e-20e3aea83e1a)

### 🔗 Data Integration Model
- After exploring the data in the **bronze layer**, I identified columns in each table that can be linked with other tables within the data warehouse.
- This analysis also helped in understanding the purpose and structure of each table.
- This model helped clean the data in the **silver layer**.

The relations are as follows:

![image](https://github.com/user-attachments/assets/44e51822-3a05-43c1-a08c-0791a667881a)


### 🛠️ Coding and Validating
[DDL script (Silver Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/silver/ddl_silver.sql) - *Create silver tables (**Note: updated before ETL for silver layer**)*

**Verify Creation**: `Object Explorer → Databases → Datawarehouse → Tables`

![image](https://github.com/user-attachments/assets/ee4d86b7-b6fd-4756-8366-62c3c0513ddb)


[Data Quality Check (Bronze Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/tests/data_quality_check_bronze.sql) - *Test bronze layer data integrity before cleaning*   

[ETL Stored Procedure (Silver Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/silver/proc_load_silver.sql) - *Extract, Transform & Load cleaned data into silver layer (**Note: updated data types in silver DDL before running**)*

`Verify Creation: Object Explorer → Databases → Datawarehouse → Programmability → Stored Procedures`

![image](https://github.com/user-attachments/assets/f4b3de9b-4529-4678-bb99-edd3e39bba88)

***Usage**: Running the following code executes the `silver.load_silver` stored procedure (It takes raw data from the Bronze layer → cleans and transforms it → loads the refined data into tables under the silver schema).*
```sql 
EXEC silver.load_silver; 
```

[Data Quality Check (Silver Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/tests/data_quality_check_silver.sql) - *Test silver layer data integrity after ETL for silver layer*

---
##  🥇 Gold Layer (Business-Ready Data)

*The Gold Layer is the curated data layer, where cleaned data from the Silver Layer is modeled into dimension and fact tables to support reporting, analytics, and decision-making. In this layer I created a **Logical Data Model** (Star Schema) which defines Business Entities / Objects (Sales, Products, Customers), their Columns and their Relationships.*  


Steps followed during the Gold Layer development: 

![image](https://github.com/user-attachments/assets/1e5bcaf9-f085-477a-99b2-44d87eba164c)

### 🔗 Data Integration Model (Revised)

Here is a revised model after identifying **Business Objects** (Sales, Product, Customer):

![image](https://github.com/user-attachments/assets/a92700d5-1730-45a8-8616-e60c148c5960)  

### 🛠️ Coding and Validating

[Exploratory Checks](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/gold/exploratory_checks.sql) - *Exploratory checks on Silver Layer to ensure data quality before building Gold Layer Views (dimensions & fact tables - Star Schema)*

[DDL Script (Gold Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/gold/ddl_gold.sql) - *Create Gold Views*

**Verify Creation**: `Object Explorer → Databases → Datawarehouse → Views`

![image](https://github.com/user-attachments/assets/c2900197-f9cc-4d4e-afbc-b6f70547e6c9)

***Usage**: These views can be queried directly for analytics and reporting.*

[Data Quality Check (Gold Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/tests/data_quality_check_gold.sql) - *Test gold layer data integrity*


### ⭐ Data Model (Star Schema)

The following data model includes a central Fact Table, `fact_sales`, surrounded by Dimension Tables `dim_customers` and `dim_products`. This structure is optimized for analytical queries, allowing exploration of sales data based on customer and product attributes.

![image](https://github.com/user-attachments/assets/04732813-bfe6-4694-a6ba-e1f694c5c822)

* ***Fact Table**: Stores measurable business data (e.g., sales, quantity)*
* ***Dimension Tables**: Stores descriptive attributes (e.g., customer name, product type)*
* ***Entity Relation**: `1 Mandatory to Many Optional"`. Eg: `1 Mandatory`: The customer must exist in `dim_customers`. `to Many Optional`: (a) Customers who din't place orders (b) Customers who placed only one order (c) Customers who placed multiple orders in `fact_sales`*
---
### ➡️ Data Flow Diagram

This diagram outlines how data flows through different layers. The data lineage helps to under the origin of the data:

![image](https://github.com/user-attachments/assets/c53e32c3-e676-4500-a264-7d5f297b15aa)

---
### 📘 Data Catalog

The following Data Catalog provides a detailed overview of the Gold Layer in the data warehouse. It defines the structure, purpose, and key columns of the dimension and fact tables — `dim_customers`, `dim_products`, and `fact_sales`. This catalog serves as a reference for understanding the business-level data used in analytical reporting.

[Data Catalog (Gold Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/docs/data_catalog.md)

---
> **Note:** All diagrams in this project were created using [draw.io](https://www.drawio.com/).

> [Project Home Page](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/tree/main?tab=readme-ov-file) – Explore the full SQL Data Warehouse Project, including documentation, scripts, and visualizations.

