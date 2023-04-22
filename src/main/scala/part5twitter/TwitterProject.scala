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

  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

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

  /*
    Exercise
    1. Compute average tweet length in the past 5 seconds, every 5 seconds. Use a window function.
    2. Compute the most popular hashtags (hashtags most used) during a 1 minute window,update every 10 seconds.
   */

  def getAverageTweetLength(): DStream[Double] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)
    tweets
      .map(status => status.getText)
      .map(text => text.length)
      .map(len => (len, 1))
      .reduceByWindow((tuple1, tuple2) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2), Seconds(60), Seconds(10))
      .map { megaTuple =>
        val tweetLengthSum = megaTuple._1 // len of the tweets --> .map(len => (len, 1))
        val tweetCount = megaTuple._2 // count of the tweets  --> .map(len => (len, 1))

        tweetLengthSum * 1.0 / tweetCount // tweetLengthSum is Int and i want to return Double so multiple with 1.0
      }
  }

  def main(args: Array[String]): Unit = {
    //readTwitter()
    getAverageTweetLength().print() // if we need to use .print() then method must return 'DStream'
    ssc.start()
    ssc.awaitTermination()
  }
}
