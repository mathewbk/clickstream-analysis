// Databricks notebook source
// MAGIC %sh cat /etc/issue

// COMMAND ----------

// MAGIC %python
// MAGIC jdbcHostname = "35.226.192.198"
// MAGIC jdbcPort = "3306"
// MAGIC jdbcDatabase = "sys"
// MAGIC username = "bmathew"
// MAGIC password = "Password1"
// MAGIC driver = "com.mysql.jdbc.Driver"
// MAGIC 
// MAGIC jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
// MAGIC connectionProperties = {"user" : username, "password" : password, "driver" : driver}
// MAGIC 
// MAGIC pushdown_query = "(SELECT * FROM sys.version) my_data"
// MAGIC df2 = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
// MAGIC df2.show()

// COMMAND ----------

// MAGIC %python
// MAGIC import sys
// MAGIC sys.version_info

// COMMAND ----------

# Databricks notebook source
import org.apache.spark.sql.SparkSession

object TestDeltaJar {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Test Delta From Scala Jar").getOrCreate()
    val filename = args(0)
    val df1 = spark.read.format("csv").load(filename).limit(1000)
    df1.write.format("delta").mode("overwrite").save("/tmp/bkm/delta/test_sbt/write")
  }
}
