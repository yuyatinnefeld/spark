package com.spark_streaming.kafka

import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition

object KafkaSpark1 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "rddBasic")
    val ssc = new StreamingContext(sc, Seconds(5))

    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List("spark_kafka", "topicYY")

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-notes",
      "auto.offset.reset" -> "earliest"
    )

    val offsets = Map(new TopicPartition("spark_kafka", 0) -> 2L)

    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets))

     dstream.foreachRDD { rdd =>
       // Get the offset ranges in the RDD
       val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
       for (o <- offsetRanges) {
         println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
       }
     }

    ssc.start

    ssc.awaitTermination()

  }
}
