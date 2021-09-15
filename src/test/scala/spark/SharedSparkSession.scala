package spark

import org.apache.spark.sql.SparkSession

object SharedSparkSession {
  val sparkSession = SparkSession
    .builder()
    .appName("SparkSession for unit tests")
    .master("local[*]")
    .config("spark.files.overwrite","true")
    .getOrCreate()
}
