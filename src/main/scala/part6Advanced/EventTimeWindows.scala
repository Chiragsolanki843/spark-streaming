package part6Advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchaseFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*") // build the four column in final dataset

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchaseFromSocket()

    val windowsByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window : sliding duration == window duration
      // column name, windowDuration, slideDuration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowsByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchaseFromSocket()

    val windowsByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column : has fields {start, end}
      // column name, windowDuration, slideDuration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowsByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregatePurchasesBySlidingWindow()
  }
}
