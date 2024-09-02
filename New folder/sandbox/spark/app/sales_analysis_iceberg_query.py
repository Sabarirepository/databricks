from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, broadcast
import sys
import os

def analyze_sales():
    # Create a Spark session with Iceberg support
    spark = SparkSession.builder \
        .appName("SalesAnalysis") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

    try:
        
        # Query Iceberg tables
        df_vendor1 = spark.sql("SELECT * FROM local.default.vendorBucket WHERE status = 'A'")

        print("Filtered Vendor Data:")
        df_vendor1.show()
        
        # Query Iceberg tables
        df_vendor1 = spark.sql("SELECT * FROM local.default.vendorBucket")

        print("Filtered Vendor Data:")
        df_vendor1.show()

        # Show tables in the local catalog
        print("Tables in local.default:")
        spark.sql("SHOW TABLES IN local.default").show()

        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    analyze_sales()
    print("Job finished")