package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ResponseHeaderOverrides
import com.amazonaws.services.s3.model.S3Object

import scala.util._

object WordCount {
  implicit class OptionOps[A](opt: Option[A]) {
    def toTry(msg: String): Try[A] = {
      opt
        .map(Success(_))
        .getOrElse(Failure(new NoSuchElementException(msg)))
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder().getOrCreate()
    val s3 = AmazonS3Client.builder()
    s3.setRegion("eu-west-1")

    println(s3.build().listBuckets().get(0).getName)

    println(s"sys.env = ${sys.env}")
    println(s"spark conf = ${spark.conf.getAll}")
    val res = for {
      inputFile <- sys.env.get("INPUT").toTry("input parameter not found")
      outputFile <- sys.env.get("OUTPUT").toTry("output parameter not found")
    } yield countWord(inputFile, outputFile)
    if (res.isFailure) {
      throw res.failed.get
    }
    else {
      println("Run with success !")
    }
  }

  def countWord(inputFile: String, outputFile: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println(s"input = $inputFile")
    println(s"output = $outputFile")

    val rdd = spark.sparkContext.textFile(inputFile)
    val words = rdd.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.toDF("word", "count").write.mode(SaveMode.Overwrite).parquet(outputFile)
  }
}

