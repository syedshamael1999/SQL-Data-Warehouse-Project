# Data Warehouse Project Walkthrough

### 🧠 Datawarehouse Approach Selection
![image](https://github.com/user-attachments/assets/38fa962a-bdfd-46bd-8ece-81897d82593c)  

- The approach chosen was the **Medallion Architecture**


** EXPLAIN WHY !!!!!!!!!!!!111
---
### 🏛️ Data Architecture

![image](https://github.com/user-attachments/assets/6b34206f-19b4-4e56-96b9-a309ee212b6f)

---

### 📑 Layer Specifications
![image](https://github.com/user-attachments/assets/b77d738e-cb06-45fb-8cc1-608d169c75bc)  

---
### 🗄️ Creating Database and schema

"""" GIVE LINK TO SCRIPT""""

---

### 🔄 ETL (Extraction, Transformation, Load)

The highlighted components in the image below are the ones implemented in this project:

![image](https://github.com/user-attachments/assets/03755803-1c9e-4cd1-a641-d4d2c1f0791c)  


---
##  🥉 Bronze Layer
![image](https://github.com/user-attachments/assets/f0f5a534-33da-4963-b05a-15e020322df0)  

Source System Interview Topics are as follows:

![image](https://github.com/user-attachments/assets/85877495-4a59-488c-8d3c-a9ffd0d92004)

** LINK TO BRONZE DDL
** LINK TO BRONZE PROC
---
## 🥈 Silver Layer 
![image](https://github.com/user-attachments/assets/0b0f0302-f4d9-4a30-b09e-20e3aea83e1a)

### 🔗 Data Integration Model
- After analyzing the data in the **bronze layer**, I identified columns in each table that can be linked with other tables within the data warehouse.
- This analysis also helped in understanding the purpose and structure of each table.
- This model helped clean the data in the **silver layer**.

The relations are as follows:

![image](https://github.com/user-attachments/assets/44e51822-3a05-43c1-a08c-0791a667881a)


### 🛠️ Coding and Validating
[Link to DDL script (Silver Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/silver/ddl_silver.sql) - *Create silver Tables (**Note: updated before inserting data into silver layer**)*

[Link to Data Quality Check - Bronze Layer](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/tests/data_quality_check_bronze.sql) - *Test bronze layer data integrity before cleaning*   

[Link to Silver Layer Stored Procedures](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/scripts/silver/proc_load_silver.sql) - *Inserts cleaned data into silver layer (**Note: update data types in silver DDL**)*

[Link to Data Quality Check (Silver Layer)](https://github.com/syedshamael1999/SQL-Data-Warehouse-Project/blob/main/tests/data_quality_check_silver.sql) - *Test silver layer data integrity after cleaning and inserting data into silver layer*


---
##  🥇 Gold Layer 
![image](https://github.com/user-attachments/assets/1e5bcaf9-f085-477a-99b2-44d87eba164c)

### 🔗 Data Integration Model (Revised)

The revised model with **Business Objects** are as follows:

![image](https://github.com/user-attachments/assets/a92700d5-1730-45a8-8616-e60c148c5960)  

### Data Model (Sales Data Mart - Star Schema)
![image](https://github.com/user-attachments/assets/04732813-bfe6-4694-a6ba-e1f694c5c822)

---
### ➡️ Data Flow Diagram
![image](https://github.com/user-attachments/assets/c53e32c3-e676-4500-a264-7d5f297b15aa)
