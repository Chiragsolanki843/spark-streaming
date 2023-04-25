package part6Advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingTimeWindows {

  val spark = SparkSession.builder()
    .appName("Processing Time Windows")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def aggregateByProcessingTime() = {
    val linesCharCountByWindowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(col("value"), current_timestamp().as("ProcessingTime"))
      .groupBy(window(col("ProcessingTime"), "10 seconds").as("window"))
      .agg(sum(length(col("value"))).as("charCount")) // counting character every 10 seconds by processing time
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("charCount")
      )

    linesCharCountByWindowDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    /*
    +-------------------+-------------------+---------+
    |              start|                end|charCount|
    +-------------------+-------------------+---------+
    |2023-04-25 14:53:40|2023-04-25 14:53:50|      292|
    +-------------------+-------------------+---------+

     */
  }

  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }
}

// keep in mind : Window duration and sliding interval must be a multiple of the batch interval
// otherwise spark will through the exception while executing time