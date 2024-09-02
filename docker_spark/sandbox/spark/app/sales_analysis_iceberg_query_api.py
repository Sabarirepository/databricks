from flask import Flask, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

app = Flask(__name__)

# Global variable for cached DataFrame
cached_vendor_df = None

def get_spark_session():
    """Create and return a Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("SalesAnalysis") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/opt/bitnami/spark/resources/iceberg_warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

def get_cached_vendor_data(spark):
    """Get or create cached vendor data."""
    global cached_vendor_df
    if cached_vendor_df is None:
        # Query Iceberg tables
        df_vendor1 = spark.sql("SELECT * FROM local.default.vendorBucket WHERE status = 'A'")
        df_vendor1.cache()
        df_vendor1.count()  # Materialize the cache
        cached_vendor_df = df_vendor1
    return cached_vendor_df

@app.route('/vendor_data', methods=['GET'])
def get_vendor_data():
    """Endpoint to get filtered vendor data."""
    spark = get_spark_session()
    try:
        # Get cached vendor data
        df_vendor = get_cached_vendor_data(spark)
        
        # Convert to Pandas DataFrame for easy JSON conversion
        vendor_data = df_vendor.toPandas().to_dict(orient='records')
        
        return jsonify(vendor_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        # Do not stop the Spark session here if you want to reuse it across requests
        pass

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)