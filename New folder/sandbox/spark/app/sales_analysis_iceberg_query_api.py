from flask import Flask, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, broadcast
import os

app = Flask(__name__)

def get_spark_session():
    """Create and return a Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("SalesAnalysis") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

@app.route('/vendor_data', methods=['GET'])
def get_vendor_data():
    """Endpoint to get filtered vendor data."""
    spark = get_spark_session()
    try:
        # Query Iceberg tables
        df_vendor1 = spark.sql("SELECT * FROM local.default.vendorBucket WHERE status = 'A'")
        
        # Convert to Pandas DataFrame for easy JSON conversion
        vendor_data = df_vendor1.toPandas().to_dict(orient='records')
        return jsonify(vendor_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        spark.stop()

@app.route('/tables', methods=['GET'])
def show_tables():
    """Endpoint to show tables in the local catalog."""
    spark = get_spark_session()
    try:
        # Show tables in the local catalog
        tables = spark.sql("SHOW TABLES IN local.default").toPandas().to_dict(orient='records')
        return jsonify(tables)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        spark.stop()

if __name__ == "__main__":
    app.run(debug=True)