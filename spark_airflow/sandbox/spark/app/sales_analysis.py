from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

def analyze_sales(input_path, output_path):
    # Create a Spark session
    spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

    # Read the CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Calculate total sales by product
    sales_by_product = df.groupBy("product_id", "product_name")                          .agg(sum("sale_amount").alias("total_sales"))                          .orderBy(col("total_sales").desc())

    # Show the results
    sales_by_product.show()

    # Write the results to a CSV file
    sales_by_product.write.csv(output_path, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    input_path = "/opt/bitnami/spark/resources/sales.csv"
    output_path = "/opt/bitnami/spark/resources/sales_analysis_output"
    analyze_sales(input_path, output_path)
