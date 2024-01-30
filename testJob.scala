// Databricks notebook source
var filename = dbutils.widgets.get("filename")
val filePath = s"/FileStore/tables/$filename.csv"
println(filePath)

var filename1 = dbutils.widgets.get("filename1")
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


