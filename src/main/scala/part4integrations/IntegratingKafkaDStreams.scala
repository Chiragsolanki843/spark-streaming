package part4integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

// for this program need to start docker
// then also nc -lk 12345
// also need consumer -> docker exec -it rockthejvm-sparkstreaming-kafka bash
// -> bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rockthejvm
// now from netcat we can received data into kafka consumer

object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map( // allow spark and kafka communicate with binary
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka  // org.apache.kafka.common.serialization
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka // import org.apache.kafka.serialization
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object] // false is not a object so we need to use asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      /*
        Distributes the partitions evenly across the Spark Cluster
        Alternative :
         - PreferBrokers if the brokers and executors are in the same cluster
         - PreferFixed
       */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1")) // group id must require if we create another stream then we need one more group id
      /*
          /*[String : key, String : value] we expect key, value would be String which pass "kafkaParams"*/
          Alternative
          - SubscribePattern (allow us to pass regex) allows subscribing to topics matching a pattern
          - Assign - advanced; Allows specifying offsets and partitions per topic
       */
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)

    // transform data
    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD { rdd => // foreachRDD means each batch
      rdd.foreachPartition { partition =>
        // inside this lambda, the code is run by a single executor

        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // producer can insert records into the kafka topics
        // available on this executor
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach { value =>
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)

          // feed the message into the Kafka topic
          producer.send(message)
        }
        producer.close()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //    readFromKafka()
    // we got data as tuple (key,value) key = null value will be msg which we send from kafka producer
    writeToKafka()
  }
}

// Remember to
// - Create the producer per partition as producers are not serializable
// - Create a Kafka message per each record
// - close the producer when you're done processing the partition










