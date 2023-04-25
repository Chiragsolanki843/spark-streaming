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

  def readFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

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

  /*
      Exercise
        1. Show the best selling product of every day, +quantity sold.
        2. Show the best selling product of every 24 hours, updated every hour.
  * */

  def bestSellingProductPerDay() = { // tumbling window
    val purchasesDF = readFromFile()

    val bestSelling = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      ).orderBy(col("day"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def bestSellingProductEvery24h() = {
    val purchasesDF = readFromFile()

    val windowsByDay = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))

      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      ).orderBy(col("start"), col("totalQuantity").desc)

    windowsByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
  /*
      For window functions, windows start at Jan 1 1970, 12 AM GMT
   */

  def main(args: Array[String]): Unit = {
    //aggregatePurchasesBySlidingWindow()
    bestSellingProductPerDay()
    //bestSellingProductEvery24h()
  }
}