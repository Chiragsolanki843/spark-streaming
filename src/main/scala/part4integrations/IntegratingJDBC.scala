package part4integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

// JDBC doesn't support read/writing streaming data but,
// Some JDBC write support, Per batch (Using though "foreachBatch")
// Reading Data is not supported format of "JDBC"

object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() = {

    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset/DataFrame
        batch.write
          .format("jdbc")
          .mode(SaveMode.Overwrite) //
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
        // if you Re-Run the code again then it will fail because "public.cars" already in postgresql
        // so use ".mode(SaveMode.Overwrite/Append/Ignore) so then it will run.
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }
}
