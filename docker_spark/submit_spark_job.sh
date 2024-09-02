#!/bin/bash


# Disable path conversion
export MSYS_NO_PATHCONV=1


# Set the correct paths
PROJECT_ROOT="/c/GitHub/docker-airflow-spark"
SPARK_RESOURCES="${PROJECT_ROOT}/sandbox/spark/resources"
SPARK_APP="${PROJECT_ROOT}/sandbox/spark/app"

# Copy the sales.csv file to the Spark master container
docker cp "${SPARK_RESOURCES}/sales.csv" docker_spark_spark-master_1:/opt/bitnami/spark/resources/sales.csv

# Copy the sales_analysis.py file to the Spark master container
docker cp "${SPARK_APP}/sales_analysis.py" docker_spark_spark-master_1:/opt/bitnami/spark/app/sales_analysis.py

# Verify that the files are copied correctly
echo "Verifying files in the container:"
docker exec docker_spark_spark-master_1 ls -l /opt/bitnami/spark/resources/
docker exec docker_spark_spark-master_1 ls -l /opt/bitnami/spark/app/

# Submit Spark job to the master
echo "Submitting Spark job:"
docker exec docker_spark_spark-master_1 \
    spark-submit \
    --master spark://spark-master:7077 \
    --name SalesAnalysis \
    /opt/bitnami/spark/app/sales_analysis_iceberg_query_api.py

# Display the results
echo "Analysis Results:"
docker exec docker_spark_spark-master_1 ls -l /opt/bitnami/spark/resources/sales_analysis_output/
docker exec docker_spark_spark-master_1 cat /opt/bitnami/spark/resources/sales_analysis_output/part-*.csv
