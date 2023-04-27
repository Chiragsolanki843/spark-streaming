package part7science

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

object ScienceSparkAggregator {

  val spark = SparkSession.builder()
    .appName("The Science Project")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  case class UserResponse(sessionId: String, clickDuration: Long)

  case class UserAverageResponse(sessionId: String, avgDuration: Double)

  def readUserResponses(): Dataset[UserResponse] = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "science")
    .load()
    .select("value")
    .as[String]
    .map { line =>
      val tokens = line.split(",")
      val sessionId = tokens(0)
      val time = tokens(1).toLong

      UserResponse(sessionId, time)
    }

  /*
     aggregate the average response time over the past 3 clicks

     1 2 3 4 5 6 7 8 9 10 // calculate three numbers from starting
     6 9 12 15 18 21 24 27

     abc - [100, 200, 300, 400, 500, 600]
     updateUserResponseTime("abc", [100, 200, 300, 400, 500, 600], Empty) => Iterator(200, 300, 400, 500)

     100 -> state becomes [100]
     200 -> state becomes [100, 200]
     300 -> state becomes [100, 200, 300] -> first average 200
     400 -> state becomes [200, 300, 400] -> next average 300
     500 -> state becomes [300, 400, 500] -> next average 400
     600 -> state becomes [400, 500, 600] -> next average 500

     Iterator will contain 200, 300, 400, 500

     Real Time :
     d7fc1916-1923-4f17-836c-907cee251e1b,1702
     d7fc1916-1923-4f17-836c-907cee251e1b,935
     d7fc1916-1923-4f17-836c-907cee251e1b,794
     d7fc1916-1923-4f17-836c-907cee251e1b,657
     d7fc1916-1923-4f17-836c-907cee251e1b,796

     window 1 = [1702, 935, 794] = 1143.6
     window 2 = [935, 794, 657] = 795.3
     window 3 = [794, 657, 796] = 749

     next batch :
     d7fc1916-1923-4f17-836c-907cee251e1b,676
     d7fc1916-1923-4f17-836c-907cee251e1b,756
     d7fc1916-1923-4f17-836c-907cee251e1b,755
     d7fc1916-1923-4f17-836c-907cee251e1b,1029
     d7fc1916-1923-4f17-836c-907cee251e1b,713
     d7fc1916-1923-4f17-836c-907cee251e1b,1100

     window 4 = [657, 796, 676] = 709.6

     // windows will start from last two click time
  */

  def updateUserResponseTime(n: Int)(
    sessionId: String,
    group: Iterator[UserResponse],
    state: GroupState[List[UserResponse]]
  ): Iterator[UserAverageResponse] = {
    group.flatMap { record =>

      val lastWindow =
        if (state.exists) state.get
        else List()

      val windowLength = lastWindow.length
      val newWindow =
        if (windowLength >= n) lastWindow.tail :+ record // " :+ " -> append
        else lastWindow :+ record

      // for Spark to give us to the state in the next batch
      state.update(newWindow)

      if (newWindow.length >= n) {
        val newAverage = newWindow.map(_.clickDuration).sum * 1.0 / n
        Iterator(UserAverageResponse(sessionId, newAverage))
      } else {
        Iterator()
      }
    }
  }

  def getAverageResponseTime(n: Int) = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updateUserResponseTime(n))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def logUserResponses() = {
    readUserResponses().writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getAverageResponseTime(3)
  }
}
