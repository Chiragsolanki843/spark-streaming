package part2structuredstreaming

import common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("Ouw first streams")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def readFromSocket() = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    // consuming a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // wait for the stream to Finish.
    query.awaitTermination()
  }

  def readFromFiles() = {

    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dataFormat", "MMM dd yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      //.trigger(Trigger.ProcessingTime(2.seconds)) // every 2 seconds run the query means in this time interval new batch created every time
      //.trigger(Trigger.Once()) // single batch, then terminate // its too fast it will nothing show in output
      .trigger(Trigger.Continuous(2.seconds)) // experimental, every 2 seconds create a batch with whatever you have continuously
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    //readFromSocket()
    //readFromFiles()
    demoTriggers()
  }
}













