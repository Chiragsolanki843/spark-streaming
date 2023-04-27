package part7science

import org.apache.spark.sql.{Dataset, SparkSession}

object ScienceSparkAggregator {

  val spark = SparkSession.builder()
    .appName("The Science Project")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  case class UserResponse(sessionId: String, clickDuration: Long)

  def readUserResponses(): Dataset[UserResponse] = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "science")
    .load()
    .select("value")
    .as[String]
    .map { line =>
      val tokens = line.split(",")
      val sessionId = tokens(0)
      val time = tokens(1).toLong

      UserResponse(sessionId, time)
    }

  /*
     aggregate the average response time over the past 10 clicks

  */


  def logUserResponses() = {
    readUserResponses().writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    logUserResponses()
  }
}
