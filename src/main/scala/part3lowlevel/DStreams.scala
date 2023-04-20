package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /*
    Spark Streaming Context = entry point to the DStreams API
    - needs the spark context (sparkContext)
    - a duration = batch interval (Seconds(1))
  */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
   - define input sources by creating DStreams
   - define transformations on DStreams
   - call an action on DStreams
   - start ALL computations with ssc.start() (we can have multiple ssc)
   - no more computations can be added
   - await termination, or stop the computation
   - you cannot restart a computation

   - every second micro batches created whether we have data or not.
  */

  def readFromSocket() = {

    // DStream -> is distributed collection of String
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = are lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action = print()
    // wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words") // each folder = RDD = batch, each file = a partition of the RDD

    ssc.start() // we need start and awaitTermination to run any computations
    ssc.awaitTermination()
  }

  def createNewFile() = {

    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where I will store a new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,May 1 2000,21
          |AAPL,Jun 1 2000,26.19
          |AAPL,Jul 1 2000,25.41
          |AAPL,Aug 1 2000,30.47
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |""".stripMargin.trim)

      writer.close()
    }).start()
  }

  def readFromFile() = {

    createNewFile() // operates on another thread
    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"

    /*
        ssc.textFileStream monitors a directory for NEW FILES.
     */

    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dataFormat = new SimpleDateFormat("MMM d yyyy")
    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dataFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // ACTION
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
    //readFromFile()
  }
}

// Streaming Context we can create from
// - ssc.socketTextStream("localhost",12345)
// - ssc.textFileStream("folder path")

// Support various transformations
// socketStream.flatMap(_.split(" ")).map(_.length)

// Computation need to start
// socketStream.print() //action
// ssc.start() // no transformation/actions past this point
// ssc.awaitTermination()














