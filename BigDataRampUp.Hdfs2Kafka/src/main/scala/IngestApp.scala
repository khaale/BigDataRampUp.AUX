import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by Aleksander_Khanteev on 4/22/2016.
  */
object IngestApp {

  var filePath = "hdfs://sandbox.hortonworks.com:8020/data/advertising/raw-test"
  var topicName = "stream.raw"
  var brokers = "sandbox.hortonworks.com:6667"

  def main(args: Array[String]) {

    if (args.length < 3) {
      println(s"Arguments: hdfsFilePath kafkaBroker kafkaTopic. Using default values: $filePath $brokers $topicName")
    }
    else {
      filePath = args(0)
      brokers = args(1)
      topicName = args(2)
    }

    val conf = new SparkConf().setAppName("Hdfs2Kafka ingestion.")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val messagesSent = sc.accumulator(0L, "messages sent")

    //report sent messages count in a background thread
    import scala.concurrent.ExecutionContext.Implicits.global
    @volatile var terminated = false
    val counterPrintFuture = Future {
      while (!terminated) {
        Thread.sleep(10*1000)
        println(s"Messages sent: ${messagesSent.value}")
      }
      println(s"Total messages sent: ${messagesSent.value}")
    }

    //run producing
    sc.textFile(filePath)
      .foreachPartition(messages => sendToKafka(messages, messagesSent))

    //finishing everything
    terminated = true
    //noinspection LanguageFeature
    Await.ready(counterPrintFuture, 15 seconds)
    sc.stop()
  }

  def sendToKafka(messages: Iterator[String], messagesSent: Accumulator[Long]): Unit = {

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    messages.foreach(msg => {

      val data = new KeyedMessage[String, String](topicName, msg)
      producer.send(data)
      messagesSent += 1
    })

    producer.close()
  }
}
