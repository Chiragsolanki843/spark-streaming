package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import part2structuredstreaming.StreamingDataFrames.spark


object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load

    val lineCount: DataFrame = lines.selectExpr("count (*) as lineCount")

    // aggregations with distinct are not supported
    // otherwise Spark will need to keep track of EVERYTHING

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark*
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // grouping
  def groupNames(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // counting occurrence of the 'name' value
    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()
    // it will counting the names

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    /*
    -------------------------------------------
    Batch: 4
    -------------------------------------------
    +-------+-----+
    |   name|count|
    +-------+-----+
    |  Danie|    1|
    |Charlie|    2|
    |    Bob|    1|
    |  Alice|    1|
    | Daniel|    2|
    +-------+-----+
    */


  }


  def main(args: Array[String]): Unit = {

    //streamingCount()
    //numericalAggregations(stddev)
    groupNames()
  }
}


/*
def numericalAggregations(): Unit = {
  val lines: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()

  // aggregate here
  val numbers = lines.select(col("value").cast("integer").as("number"))
  val sumDF = numbers.select(sum(col("number")).as("agg_so_far"))

  sumDF.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()
}

*/

// Takeaways
/*
*       Same aggregation API as non-streaming DFs
*       --> to Keep in mind
*       - aggregations work at a micro-batch level
*       - the 'append' and 'update' output mode not supported without watermarks*
*       - some aggregations are not supported, e.g sorting, distinct or chained aggregations
* */
