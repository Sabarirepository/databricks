from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum  # Add this line
import sys

def analyze_sales(input_path_sales, input_path_vendor, output_path):
    # Create a Spark session
    #working code for spark submit
    #spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

    #spark = SparkSession.builder \
    #    .appName("SalesAnalysis") \
    #    .config("spark.driver.bindAddress", "0.0.0.0") \
    #    .config("spark.executor.instances", "2") \
    #    .getOrCreate()
    """
    spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .getOrCreate()
    
    
    spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.memoryOverhead", "256m") \
    .config("spark.cores.max", "2") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()
    
    """
    
    spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.memoryOverhead", "256m") \
    .config("spark.cores.max", "2") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.driver.memory", "256m") \
    .config("spark.driver.cores", "1") \
    .getOrCreate()
    
    
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

    # Join the DataFrames
    #df_joined = df_sales.join(df_vendor, "product_id")
    #19 s
    df_joined = df_sales.join(broadcast(df_vendor), "product_id")

    # Group by product and calculate total sales
    sales_by_product = df_joined.groupBy("product_id", "product_name", "vendor_name") \
                         .agg(sum("sale_amount").alias("total_sales")) \
                         .orderBy(col("total_sales").desc())

    # Show the results
    print("Sales Analysis Result:")
    sales_by_product.show()

    # Write the results to a CSV file
    sales_by_product.write.csv(output_path, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    input_path_sales = "/opt/bitnami/spark/resources/sales.csv"
    input_path_vendor = "/opt/bitnami/spark/resources/product_details.csv"
    output_path = "/opt/bitnami/spark/resources/sales_analysis_output"
    #input_path = "/opt/airflow/spark/resources/sales.csv"
    #output_path = "/opt/airflow/spark/resources/sales_analysis_output"
    print("input_path:",input_path_sales)
    analyze_sales(input_path_sales, input_path_vendor, output_path)
    print("Job finished")