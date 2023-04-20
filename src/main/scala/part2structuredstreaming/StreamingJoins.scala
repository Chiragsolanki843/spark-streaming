package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val guitarPlayer = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")
  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  // joining static DFs
  val joinCondition = guitarPlayer.col("band") === bands.col("id")
  val guitaristBands = guitarPlayer.join(bands, joinCondition, "inner")

  val bandsSchema = bands.schema // when spark read DF of bands that time it will auto infrared the schema and we can use here

  def joinStreamWithStatic() = {

    val streamBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column 'value' of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")
    // join happens PER BATCH
    val streamedBandsGuitaristsDF = streamBandsDF.join(guitarPlayer, guitarPlayer.col("band") === streamBandsDF.col("id"), "inner")

    /*
    *   restricted join :
    *   - stream joining with static : RIGHT outer join/full outer join/ right_semi not permitted with static DFs
    *   - static joining with streaming : LEFT outer join/full outer join/left_semi not permitted
    * */

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append") //
      .start()
      .awaitTermination()
  }

  // since Spark 2.3 we have stream vs stream joins
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayer.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    // join stream with stream
    val streamedJoin = streamedBandsDF.join(streamedGuitaristDF, streamedGuitaristDF.col("band") === streamedBandsDF.col("id"), "inner")

    /*
    *  - inner joins are supported
    *  - left/right outer joins ARE supported, but MUST have watermarks (its very advance)
    *  - full outer joins are NOT supported
    * */

    streamedJoin.writeStream
      .format("console")
      .outputMode("append") // only append supported for stream vs stream joining
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //joinStreamWithStatic()
    joinStreamWithStream()
  }
}













