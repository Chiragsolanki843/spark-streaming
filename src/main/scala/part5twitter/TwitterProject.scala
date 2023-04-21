package part5twitter

import twitter4j.Status // to manage our twitters we have to import this library.
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterProject {

  val spark = SparkSession.builder()
    .appName("The Twitter Project")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readTwitter(): Unit = {

    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
    val tweets: DStream[String] = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText
      s"User $username ($followers followers) says: $text"
    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readTwitter()
  }
}
