package de.smartsquare.spark.speed

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import de.smartsquare.spark.util.SparkPipelineConfigUtil._


object SparkStreamingKafkaConsumer extends App {

  val brokers = bootstrapServers

  val sparkConf = new SparkConf().setAppName("KafkaDirectStreaming").setMaster(sparkMaster)
    .set("spark.cassandra.connection.host", hosts(0))
    .set("spark.cassandra.auth.username", username)
    .set("spark.cassandra.auth.password", password)
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  ssc.checkpoint("checkpointDir")

  val topicsSet = Set(kafkaTopic(0))
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> "spark_streaming")
  val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

  val tweets: DStream[String] = messages.map { case (key, message) => message }
  ViewHandler.createAllView(ssc.sparkContext, tweets)
  ssc.start()
  ssc.awaitTermination()
}
