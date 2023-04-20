package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import common.{Car, carsSchema}
import org.apache.spark.Partition
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._ // cassandra connector library

// need docker-compose up
// execute cql.sh --> docker exec -it rockthejvm-sparkstreaming-cassandra cqlsh
// create keyspace public with replication = { 'class':'SimpleStrategy', 'replication_factor' : 1 };
//  create table public.cars("Name" text primary key, "Horsepower" int);
// truncate public.cars; // remove all data from table
object IntegratingCassandra {

  val spark = SparkSession.builder()
    .appName("Integrating Cassandra")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  def writeStreamToCassandraInBatches() = {

    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch {
        (batch: Dataset[Car], _: Long) =>
          // save this batch to Cassandra in a single table write
          batch.select(col("Name"), col("Horsepower"))
            .write
            .cassandraFormat("cars", "public") // type enrichment
            .mode(SaveMode.Append)
            .save()
      }
      .start()
      .awaitTermination()
  }

  class CarCassandraForeachWriter extends ForeachWriter[Car] {

    /*
      - on every batch, on every partition 'partitionId'
        - on every "epoch" = chunk of data
          - call the open method; if false, skip this chunk
          - for each entry in this chunk, call the process method
          - call the close method either at the end of the chunk or with an error if it was thrown
     */

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open connection")
      true
    }

    override def process(car: Car): Unit = {
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |insert into $keyspace.$table("Name","Horsepower")
             |values ('${car.Name}', ${car.Horsepower.orNull}) // Horsepower is type of Option[Long] so we need to do ".orNull" if null value there then
             |""".stripMargin)
        //  if String then put value under the 'single quote' and if number then no need to put any quote
      }
    }

    override def close(errorOrNull: Throwable): Unit = println("Closing Connection")
  }


  def writeStreamToCassandra() = {

    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    //writeStreamToCassandraInBatches()
    writeStreamToCassandra()
  }
}
