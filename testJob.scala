// Databricks notebook source
// var filename = dbutils.widgets.get("filename")
var filename = "test/Batting"
val filePath = s"/FileStore/tables/$filename.csv"
println(filePath)
var filename1 = "test/Batting1"
// var filename1 = dbutils.widgets.get("filename1")
val filePath1 = s"/FileStore/tables/$filename1.csv"
println(filePath1)

// read csv
val batsmandf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(filePath)
val batsmandf1 = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(filePath1)

// create database if not exists
// spark.sql("CREATE DATABASE IF NOT EXISTS usmart")

// switch to the usmart database
spark.sql("USE usmart")

// save DataFrame as table
batsmandf.write.mode("overwrite").saveAsTable("usmart.result1")
batsmandf1.write.mode("overwrite").saveAsTable("usmart.result2")

var sql_query ="SELECT PlayerCountry, TotalRuns FROM result1 LIMIT 20"
val results = spark.sql(sql_query)
display(results)

// COMMAND ----------

val res_df = spark.sql("select * from usmart.result1 limit 2")
display(res_df)

// COMMAND ----------

val res_df = spark.sql("select * from usmart.result2 limit 2")
display(res_df)

// COMMAND ----------

val  r2_bronze = spark.sql("select r2.* from usmart.result1 r1 join usmart.result2 r2 on r1.PlayerCountry = r2.PlayerCountry")
display(r2_bronze)

// COMMAND ----------

val r2_notmatched = spark.sql("with tempa as (select r2.* from usmart.result2 r2 left join usmart.result1 r1 on r1.PlayerCountry = r2.PlayerCountry where r1.PlayerCountry is  null) select * from tempa")
display(r2_notmatched)

// COMMAND ----------

batsmandf.cache()
batsmandf.write.mode("overwrite").saveAsTable("r2_bronze")
batsmandf1.write.mode("overwrite").saveAsTable("r2_notmatched")

// COMMAND ----------




// Source and destination paths
val sourcePath = "/mnt/source_folder"  // Replace with your source folder path
val destinationPath = "/mnt/destination_folder"  // Replace with your destination folder path

// List files in the source folder
val fileList = dbutils.fs.ls(sourcePath)

// Move each file to the destination folder
fileList.foreach(file => {
  val sourceFile = file.path
  val destinationFile = destinationPath + "/" + file.name

  // Move the file
  dbutils.fs.mv(sourceFile, destinationFile)

  // Print information about the moved file
  println(s"Moved file from $sourceFile to $destinationFile")
})


spark.sql("SELECT * FROM live_streaming_table").show()




Yes, there are differences between a live streaming table and an autoloader in Databricks.

Live Streaming Table: A live streaming table in Databricks is created using the Delta Live Table (DLT) feature. It provides real-time access to the continuously flowing data from a streaming source. Live streaming tables are continuously synchronized with the streaming source, allowing you to query and analyze the data as it is ingested. You can perform both batch and real-time queries on live streaming tables.

Autoloader: Autoloader is a feature in Databricks that simplifies the process of loading data into a Delta table from cloud storage (such as Amazon S3 or Azure Blob Storage). It automatically detects new files added to a specified directory and loads them into a Delta table in an optimized manner. Autoloader is specifically designed for batch loading scenarios and is suitable for scenarios where new data files are periodically added to the storage location.

In summary, the main difference between a live streaming table and an autoloader is the way they handle data ingestion and processing. Live streaming tables provide real-time access to continuously flowing data from a streaming source, while autoloader is used for batch loading of data into a Delta table from cloud storage.


----------------

val filename = "/FileStore/tables/proj1/file1.csv"
val table = "my_table"

val schema = StructType(Seq(
  StructField("column1", IntegerType, nullable = true),
  StructField("column2", StringType, nullable = true)
))

val sourcedf = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .schema(schema)
  .load(filename)

sourcedf.write
  .format("delta")
  .mode("append")
  .saveAsTable(table)