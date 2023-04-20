package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // include encoders for DF => DS transformations

  import spark.implicits._
  // if you use your own implicits then no need to import above line

  def readCars(): Dataset[Car] = {
    // useful for DF -> DS transformations
    //    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column 'value'
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car] //(carsSchema) // pass explicitly Car encode
    // encoder can be passed implicitly with spark.implicits
    // need encoder(Data Structure) and that will convert row into JVM object
    // turn DataFrame rows into case class
  }

  def showCarNames() = {

    val carsDS: Dataset[Car] = readCars()

    // transformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // DF

    // collection transformation maintain type[] info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /*
  *   Exercises
  *
  *  1) Count how many POWERFUL cars we have in the DS (HP > 140)
  *  2) Average HP for the entire dataset
  *     (use the complete output mode)
  *  3)  Count the cars by Origin field
  */

  def ex1() = {

    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex2() = {

    val carsDS = readCars()
    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
    // for the aggregation complete mode is supported not append
    // average of entire data which from staring the NatCat application not select line which inserted
    // entire data since the start of the application because we use "complete" output mode
  }

  def ex3() = {
    val carsDS = readCars()
    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // option 1 dataframe
    val carCountByOriginAl = carsDS.groupByKey(car => car.Origin).count() // option 2 with the dataset API

    carCountByOriginAl
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    //showCarNames()
    //ex1()
    //ex2()
    ex3()
  }

}
// Takeaways
// pros : type safety, expressiveness, advantage of functions like map, flatMap, filter those are not in sql API
// cons : potential perf implications as lambdas cannot be optimized by spark engine


















