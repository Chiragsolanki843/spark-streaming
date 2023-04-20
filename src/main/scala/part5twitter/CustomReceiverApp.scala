package part5twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.concurrent.ExecutionContext.Implicits.global
import java.net.Socket
import scala.concurrent.{Future, Promise}
import scala.io.Source

class CustomSocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  // in production level "MEMORY_AND_DISK_2" we need to use because if some how data is not save in
  // memory then all data will save in 'DISK' and replicated twice. '2' means replicated data twice so you
  // don't loose data in cluster.

  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture = socketPromise.future

  // called asynchronously
  override def onStart(): Unit = {

    val socket = new Socket(host, port)

    // run on another thread
    Future {
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line)) // store makes this string available to Spark
    }

    socketPromise.success(socket)
  }

  // called asynchronously
  override def onStop(): Unit = socketFuture.foreach(socket => socket.close())
}

object CustomReceiverApp {

  val spark = SparkSession.builder()
    .appName("Customer receiver app")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def main(args: Array[String]): Unit = {

    val dataStream: DStream[String] = ssc.receiverStream(new CustomSocketReceiver("localhost", 12345))
    dataStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
