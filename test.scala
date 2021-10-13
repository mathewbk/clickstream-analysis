import org.apache.spark.sql.SparkSession

object TestDeltaJar {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Test Delta From Scala Jar").getOrCreate()
    val filename = args(0)
    val df1 = spark.read.format("csv").load(filename).limit(1000)
    df1.write.format("delta").mode("overwrite").save("/tmp/bkm/delta/test_sbt/write")
  }
}
