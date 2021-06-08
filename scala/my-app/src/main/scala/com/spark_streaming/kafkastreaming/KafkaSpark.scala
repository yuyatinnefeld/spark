package com.spark_streaming.kafkastreaming

import com.spark_streaming.Utilities.setupLogging
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object KafkaSpark{
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("KafkaSpark")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    setupLogging()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092, anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicYY", "topicB")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val newMap = stream.map(record => (record.timestamp(), record.value(), record.value.length))

    newMap.print()

    ssc.checkpoint("/Users/yuyatinnefeld/desktop/projects/spark/scala/checkpoint")
    ssc.start()
    ssc.awaitTermination()

  }
}
