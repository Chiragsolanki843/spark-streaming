package part1recap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkRecap {

  // the entry point to the Spark Structured API
  val spark = SparkSession.builder()
    .appName("")
    .master("local[12]")
    //.config("spark.master", "local") we can use above
    .getOrCreate()

  // read a DF
  val cars = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars")

  import spark.implicits._ // have numbers of encoder so rows of DFs are safely converting in JVM (object)

  // select
  val usefulCarsData = cars.select(
    col("Name"), // column object
    $"Name", // another column object ($ sign need spark implicits)
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("weight_in_lbs / 2.2").as("Weight_in_kg")
  )

  val carsWeights = cars.selectExpr("weight_in_lbs / 2.2")

  // filter
  val europeanCars = cars.filter(col("Origin") =!= "USA")
  val europeanCars1 = cars.where(col("Origin") =!= "USA")

  // aggregations
  val averageHP = cars.select(avg(col("Horsepower")).as("Average_HP"), count(col("Acceleration"))) // sum, mean, stddev, min, max, avg, count

  // grouping
  val countByOrigin = cars
    .groupBy(col("Origin")) // a RelationalGroupedDataset
    .count()

  // joining
  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val guitaristBands = guitarPlayers.join(bands, guitarPlayers.col("bands") === bands.col("id"), "inner")

  /*
  *   join types
  *   - inner join => only the matching rows are kept
  *   - left/right/full outer Join =>
  *   - semi/anti
  * */

  // Datasets (type[] Distributed collection of JVM(object))
  // benefits of DSs is type[] safety also its imp for functions programmer.
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  val guitarPlayerDS = guitarPlayers.as[GuitarPlayer] // only possible when we impost spark.implicits
  guitarPlayerDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  // low-level API : RDDs (typed[] distributed collection of object must like Datasets except they don't have SQL api they have function API)
  // under the hood all spark job are boil down into RDD (means convert in RDD when execute)
  val sc = spark.sparkContext
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 100000)


  // functional operators
  val double = numbersRDD.map(_ * 2) // we have map, flatMap, filter

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("number")
  // you lose type[] information bcz DFs don't have types[], you will get SQL capabilities.

  // RDD -> DS
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD)
  // this also kept type[] information and SQL capabilities

  // DS --> RDD
  val guitarPlayersRDD = guitarPlayerDS.rdd

  // DF --> RDD
  val carsRDD = cars.rdd // RDD[Row] rows are untyped collections of arbitory information.


  def main(args: Array[String]): Unit = {
    // showing a DF to the console
    // cars.show
    //cars.printSchema()
    
  }
}



















