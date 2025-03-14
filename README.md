## 📌 Enterprise Data Processing Pipeline (PySpark)  

### 🔍 Overview  
The **Enterprise Data Processing Pipeline** is a scalable **ETL (Extract, Transform, Load) pipeline** built using **AWS S3, PySpark, and MySQL**. It automates data ingestion, transformation, and loading for analytical use cases, focusing on **insurance claim processing**.  

### 🚀 Features  
✅ **Automated Data Ingestion**: Extracts insurance data from CSV files stored in AWS S3.  
✅ **PySpark-based Transformations**: Categorization, risk assessment, and aggregations.  
✅ **Database Integration**: Stores processed data in **MySQL Workbench**.  
✅ **Cloud-Native**: Uses AWS S3 and PySpark for efficient large-scale processing.  

---

## 📊 Architecture  

This pipeline follows a structured **ETL workflow**:

1️⃣ **Data Ingestion**: Raw insurance data is uploaded to AWS S3.  
2️⃣ **Data Extraction**: PySpark reads the data from **CSV files**.  
3️⃣ **Data Transformation**:  
   - **Age Group Categorization**: Young, Middle-aged, Senior  
   - **Policy Duration Calculation**  
   - **Risk Scoring**: Based on claim status  
   - **Aggregations**: Average premium, total claims, total premium  
4️⃣ **Data Loading**: Transformed data is stored in **MySQL Workbench** for analysis.  

### 🛠 High-Level Flow  
```plaintext
[S3 (Raw Data)] → [Extract with PySpark] → [Transform Data] → [Load into MySQL]
```

---

## 🏗 Technology Stack  

| Technology | Purpose |
|------------|---------|
| **AWS S3** | Cloud storage for raw and processed data |
| **PySpark** | Data transformation and processing |
| **MySQL** | Structured data storage |
| **Boto3** | AWS SDK for Python (S3 interactions) |

---

## 🛠 Setup Instructions  

### 1️⃣ Prerequisites  
Ensure you have the following installed:  
- **Python 3.x**  
- **Apache Spark (PySpark)**  
- **MySQL Workbench**  
- **AWS CLI** configured with credentials  

### 2️⃣ Clone the Repository  
```sh
git clone https://github.com/joydeep-ghosh/enterprise-data-pipeline.git  
cd enterprise-data-pipeline  
```

### 3️⃣ Install Dependencies  
```sh
pip install boto3 pyspark mysql-connector-python  
```

### 4️⃣ Configure AWS & MySQL  
- Update `config.py` with your **AWS credentials** and **MySQL connection details**.

### 5️⃣ Run the ETL Pipeline  
```sh
python etl_pipeline.py  
```

---

## 📌 Data Schema  

### 🔹 **Raw Data (CSV Format)**
| id | name  | age | state       | issue_date | claim_status | premium_amount |
|----|-------|-----|------------|------------|--------------|---------------|
| 1  | Alice  | 25  | New York   | 2020-05-12 | Approved     | 450.00        |
| 2  | Bob    | 45  | California | 2018-09-23 | Denied       | 320.00        |

### 🔹 **Transformed Data (Stored in MySQL)**  
```sql
CREATE TABLE processed_insurance_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT,
    state VARCHAR(255),
    issue_date DATE,
    claim_status VARCHAR(50),
    premium_amount FLOAT,
    age_group VARCHAR(50),
    policy_duration INT,
    risk_score INT,
    avg_premium FLOAT,
    total_claims INT,
    total_premium FLOAT
);
```

### 🔹 **Processed Data Sample**
| id | name  | age | state       | issue_date | claim_status | premium_amount | age_group | policy_duration | risk_score | avg_premium | total_claims | total_premium |
|----|-------|-----|------------|------------|--------------|---------------|-----------|----------------|------------|-------------|--------------|---------------|
| 1  | Alice  | 25  | New York   | 2020-05-12 | Approved     | 450.00        | Young     | 5 years       | 3          | 400.00      | 120          | 54000.00      |
| 2  | Bob    | 45  | California | 2018-09-23 | Denied       | 320.00        | Middle-aged | 7 years     | 1          | 350.00      | 80           | 28000.00      |

---
### 🔹 **Output**

Extracted Data:
+--------------------+-----------------+---+-----------+--------------+------------+-------------+----------+----------+
|           policy_id|    customer_name|age|policy_type|premium_amount|claim_status|         city|     state|issue_date|
+--------------------+-----------------+---+-----------+--------------+------------+-------------+----------+----------+
|7c5cb5e9-a58b-4e4...|       Ryan White| 49|     Travel|           913|    Approved|     Kingbury|   Georgia|2017-12-04|
|67ee8fe8-86b0-4c3...|      Joseph Cook| 65|     Travel|          3819|     Pending|  Williamtown|California|2017-05-04|
|91628a6a-81dc-49e...|Christopher Miles| 38|       Life|           204|     Pending|        Wuton|  Illinois|2025-01-04|
|9c779940-a844-406...|        Blake Cox| 64|       Auto|           974|      Denied|New Rickyside|California|2022-11-12|
|ccd80a86-1ef4-42c...| Carolyn Stephens| 71|       Life|          3754|     Pending|   Port Diane|  New York|2017-12-09|
+--------------------+-----------------+---+-----------+--------------+------------+-------------+----------+----------+
only showing top 5 rows

Transformed Data:
+----------+--------------------+-----------------+---+-----------+--------------+------------+-------------+----------+-----------+---------------+----------+------------------+------------+-------------+
|     state|           policy_id|    customer_name|age|policy_type|premium_amount|claim_status|         city|issue_date|  age_group|policy_duration|risk_score|       avg_premium|total_claims|total_premium|
+----------+--------------------+-----------------+---+-----------+--------------+------------+-------------+----------+-----------+---------------+----------+------------------+------------+-------------+
|   Georgia|7c5cb5e9-a58b-4e4...|       Ryan White| 49|     Travel|           913|    Approved|     Kingbury|2017-12-04|Middle-aged|              8|         3|2547.3735266008284|       12556|     31984822|
|California|67ee8fe8-86b0-4c3...|      Joseph Cook| 65|     Travel|          3819|     Pending|  Williamtown|2017-05-04|     Senior|              8|         2|2547.5227714748785|       12340|     31436431|
|  Illinois|91628a6a-81dc-49e...|Christopher Miles| 38|       Life|           204|     Pending|        Wuton|2025-01-04|Middle-aged|              0|         2| 2558.821805111821|       12520|     32036449|
|California|9c779940-a844-406...|        Blake Cox| 64|       Auto|           974|      Denied|New Rickyside|2022-11-12|     Senior|              3|         1|2547.5227714748785|       12340|     31436431|
|  New York|ccd80a86-1ef4-42c...| Carolyn Stephens| 71|       Life|          3754|     Pending|   Port Diane|2017-12-09|     Senior|              8|         2| 2550.390259635543|       12402|     31629940|
+----------+--------------------+-----------------+---+-----------+--------------+------------+-------------+----------+-----------+---------------+----------+------------------+------------+-------------+
only showing top 5 rows

Table EDPP_processed_insurance_data created successfully.
 Successfully loaded 100 records into MySQL.
 MySQL connection closed.

 <img width="621" alt="image" src="https://github.com/user-attachments/assets/22c051d5-5015-47fc-9d5f-7f4af7ceac85" />


## 📌 Future Enhancements  
🔹 Real-time data streaming using **Kafka**  
🔹 Incremental data processing for optimized performance  
🔹 Advanced data quality checks  

---

💡 **Contributions & Feedback**  
Feel free to **open issues** or **submit PRs** to enhance the project. 🚀

