
import boto3
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, year, avg, count, sum
import mysql.connector
import os
from botocore.exceptions import NoCredentialsError
import pandas as pd

# AWS Configurations
AWS_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
S3_BUCKET_NAME = "joydeep-data-bucket"
RAW_ZONE = "raw/"
STAGING_ZONE = "staging/"

# MySQL Database Configuration
MYSQL_HOST = "localhost"
MYSQL_DB = "hr"
MYSQL_USER = "root"
MYSQL_PASSWORD = "1234"

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Extract Data from CSV
def extract_data():
    """Reads insurance data from CSV file"""
    df = spark.read.csv(r"C:\Users\hp\OneDrive\Desktop\python\Big_Data\Cloned\Project\insurance_data.csv", header=True, inferSchema=True)
    print("Extracted Data:")
    df.show(5)
    return df

# Transform Data
def transform_data(df):
    """Applies transformations including categorization, risk assessment, and aggregations"""
    df = df.withColumn("age_group", 
                       when(col("age") < 30, "Young")
                       .when((col("age") >= 30) & (col("age") < 50), "Middle-aged")
                       .otherwise("Senior"))
    
    df = df.withColumn("policy_duration", year(lit("2025-01-01")) - year(col("issue_date")))
    
    df = df.withColumn("risk_score", 
                       when(col("claim_status") == "Approved", 3)
                       .when(col("claim_status") == "Pending", 2)
                       .when(col("claim_status") == "Denied", 1)
                       .otherwise(0))
    
    premium_avg_df = df.groupBy("state").agg(avg("premium_amount").alias("avg_premium"))
    claims_count_df = df.groupBy("state").agg(count("claim_status").alias("total_claims"))
    total_premium_df = df.groupBy("state").agg(sum("premium_amount").alias("total_premium"))
    
    df = df.join(premium_avg_df, on="state", how="left")
    df = df.join(claims_count_df, on="state", how="left")
    df = df.join(total_premium_df, on="state", how="left")
    
    print("Transformed Data:")
    df.show(5)
    return df


def load_data(df):
    """Loads transformed PySpark DataFrame into MySQL Workbench efficiently."""
    
    try:
        # Establish MySQL Connection
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        cursor = connection.cursor()
        
        # Drop Table if Exists
        cursor.execute("DROP TABLE IF EXISTS EDPP_processed_insurance_data")
        
        # Create Table (if not exists)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS EDPP_processed_insurance_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_name VARCHAR(255),
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
            )
        """)

        # Convert PySpark DataFrame to Pandas DataFrame
        df_pandas = df.toPandas()
        
        expected_columns = [
            "customer_name", "age", "state", "issue_date", "claim_status", "premium_amount",
            "age_group", "policy_duration", "risk_score", "avg_premium", "total_claims", "total_premium"
        ]
        
        # Select only required columns & replace NaN with None for MySQL compatibility
        df_pandas = df_pandas[expected_columns].where(pd.notna(df_pandas), None)

        # Convert DataFrame to List of Tuples for Efficient Bulk Insert
        data_to_insert = [tuple(row) for _, row in df_pandas.iterrows()]

        insert_query = f"""
            INSERT INTO EDPP_processed_insurance_data ({", ".join(expected_columns)})
            VALUES ({", ".join(["%s"] * len(expected_columns))})
        """
        print("Table EDPP_processed_insurance_data created successfully.")
            
        # Insert only 100 records
        if data_to_insert:
            cursor.executemany(insert_query, data_to_insert[:100])
            connection.commit()
            print(f" Successfully loaded {len(data_to_insert[:100])} records into MySQL.")

    except mysql.connector.Error as e:
        print(f" Error: {e}")
    
    finally:
        # Close Connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print(" MySQL connection closed.")

if __name__ == "__main__":
    extracted_df = extract_data()
    transformed_df = transform_data(extracted_df)
    load_data(transformed_df)
