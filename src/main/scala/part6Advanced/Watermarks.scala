package part6Advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp
import scala.concurrent.duration._

object Watermarks {

  val spark = SparkSession.builder()
    .appName("Late Data with Watermarks")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // 3000,blue

  import spark.implicits._

  def debugQuery(query: StreamingQuery) = {
    // useful skill for debugging stream
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s" $i : $queryEventTime")
      }
    }).start()
  }

  def testWatermarks() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        /* Constructs a Timestamp object using a milliseconds time value.
       The integral seconds are stored in the underlying date value;
        the fractional seconds are stored in the nanos field of the Timestamp object. */
        val data = tokens(1)

        (timestamp, data) // return a tuple.
      }
      .toDF("created", "color") // created : timestamp, color : String

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds") // adding a 2 second watermark
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /*
      A 2 second watermark means
      - a window will only be considered until the watermark surpass the window end
      - an element/ a row/ a record will be considered if AFTER the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) // trigger will specified when new batches are selected
      .start()
    debugQuery(query)
    query.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    testWatermarks()
  }
}

object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() // blocking call
  val printer = new PrintStream(socket.getOutputStream)

  println("socket accepted")

  def example1() = {
    Thread.sleep(7000)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,green")
    Thread.sleep(4000)
    printer.println("14000,blue")
    Thread.sleep(1000)
    printer.println("9000,red") // discarded : older than the watermark
    Thread.sleep(3000)
    printer.println("15000,red")
    printer.println("8000,blue") // discarded : older than the watermark
    Thread.sleep(1000)
    printer.println("13000,green")
    Thread.sleep(500)
    printer.println("21000,green") // discarded : bcz spark ready data till 19 second window
    Thread.sleep(3000)
    printer.println("4000,purple") // except to be dropped ; it's older than the watermark
    Thread.sleep(2000)
    printer.println("17000,green") // discarded : bcz spark ready data till 19 second window
  }

  def example2() = {
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("4000,blue")

    Thread.sleep(7000)
    printer.println("1000,yellow")
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")

    Thread.sleep(3000)
    printer.println("10000,pink")
  }

  def example3() = {
    Thread.sleep(2000)
    printer.println("9000,blue")
    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")
    Thread.sleep(2000)
    printer.println("5000,red") // discarded
    printer.println("18000,blue")
    Thread.sleep(1000)
    printer.println("2000,green") // discarded
    Thread.sleep(2000)
    printer.println("30000,purple") // discarded : bcz spark will read data till 28 sec window
    printer.println("10000,green")

  }

  def main(args: Array[String]): Unit = {
    example3()
  }
}














