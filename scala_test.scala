// Databricks notebook source
import org.apache.spark.sql.SparkSession

object TestDeltaJar {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Test Delta From Jar").getOrCreate()
    val df1 = spark.read.format("parquet").load("/delta/reports_delta/solution_id=sol1/variablvsactionset/date_wise=26-5-2018").limit(100)
    df1.write.format("delta").mode("append").save("/tmp/bmathew/delta/write")
  }
}
