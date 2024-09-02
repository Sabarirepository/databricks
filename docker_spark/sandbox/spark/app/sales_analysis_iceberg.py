from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, broadcast
import sys
import os

def analyze_sales(input_path_sales, input_path_vendor, output_path):
    # Create a Spark session with Iceberg support
    spark = SparkSession.builder \
        .appName("SalesAnalysis") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/opt/bitnami/spark/resources/iceberg_warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

    try:
        # Read the CSV files
        df_sales = spark.read.csv(input_path_sales, header=True, inferSchema=True)
        df_vendor = spark.read.csv(input_path_vendor, header=True, inferSchema=True)

        print("Sales DataFrame:")
        df_sales.show()
        print("Sales DataFrame Schema:")
        df_sales.printSchema()
        print("Sales DataFrame Partitions:", df_sales.rdd.getNumPartitions())

        print("Vendor DataFrame:")
        df_vendor.show()
        print("Vendor DataFrame Schema:")
        df_vendor.printSchema()
        print("Vendor DataFrame Partitions:", df_vendor.rdd.getNumPartitions())

        # Cast sale_amount to double, replacing any non-numeric values with null
        df_sales = df_sales.withColumn("sale_amount", 
                           when(col("sale_amount").cast("double").isNotNull(), 
                                col("sale_amount").cast("double"))
                           .otherwise(None))

        # Drop existing tables if they exist
        #spark.sql("DROP TABLE IF EXISTS local.default.salesBucket")
        #spark.sql("DROP TABLE IF EXISTS local.default.vendorBucket")

        # Create Iceberg tables
        df_sales.writeTo("local.default.salesBucket").using("iceberg").createOrReplace()
        df_vendor.writeTo("local.default.vendorBucket").using("iceberg").createOrReplace()

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

        # Join using Iceberg tables with broadcast
        df_joined = spark.table("local.default.salesBucket").join(
            broadcast(spark.table("local.default.vendorBucket")), 
            "product_id"
        )

        # Group by product and calculate total sales
        sales_by_product = df_joined.groupBy("product_id", "product_name", "vendor_name") \
            .agg(sum("sale_amount").alias("total_sales")) \
            .orderBy(col("total_sales").desc())

        # Show the results
        print("Sales Analysis Result:")
        sales_by_product.show()

        # Write the results to a CSV file
        sales_by_product.write.csv(output_path, header=True, mode="overwrite")
        print(f"Results written to {output_path}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    input_path_sales = "/opt/bitnami/spark/resources/sales.csv"
    input_path_vendor = "/opt/bitnami/spark/resources/product_details.csv"
    output_path = "/opt/bitnami/spark/resources/sales_analysis_output"

    # Check if input files exist
    if not os.path.exists(input_path_sales):
        print(f"Error: Sales input file not found at {input_path_sales}")
        sys.exit(1)
    if not os.path.exists(input_path_vendor):
        print(f"Error: Vendor input file not found at {input_path_vendor}")
        sys.exit(1)

    print("Sales input path:", input_path_sales)
    print("Vendor input path:", input_path_vendor)
    print("Output path:", output_path)

    analyze_sales(input_path_sales, input_path_vendor, output_path)
    print("Job finished")