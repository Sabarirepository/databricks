from pyspark.sql import SparkSession
import logging
import socket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    logger.info("Starting Spark session creation")
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    logger.info(f"Hostname: {hostname}, IP: {ip_address}")

    spark = SparkSession.builder \
        .appName("TestSpark") \
        .config("spark.driver.host", ip_address) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "1024m") \
        .getOrCreate()

    logger.info("Spark session created successfully")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Spark master: {spark.sparkContext.master}")

    logger.info("Creating DataFrame")
    df = spark.createDataFrame([(1, "test")], ["id", "value"])
    logger.info("DataFrame created")

    logger.info("Collecting data")
    result = df.collect()
    logger.info(f"Collected data: {result}")

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    raise

finally:
    if 'spark' in locals():
        logger.info("Stopping Spark session")
        spark.stop()
        logger.info("Spark session stopped")