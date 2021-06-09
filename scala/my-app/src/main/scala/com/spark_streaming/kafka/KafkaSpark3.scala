package com.spark_streaming.kafka

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.slf4j.LoggerFactory

object KafkaSpark3 {

  def customTransform(rdd: RDD[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]): RDD[(String, String)] = {
    rdd.map(t => (t.key(), t.value()))
  }

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass.getName)

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
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // show kafka partition meta info
        offsetRanges.foreach(t => logger.info("Topic: %s, Partition: %d, fromOffset: %d, untilOffset: %d ".format(t.topic, t.partition, t.fromOffset, t.untilOffset)))
        // add at this place custom transformations and actions
        customTransform(rdd).foreachPartition(
          iterator =>
            iterator.foreach(x => logger.info(s"### Kafka - Key: ${x._1} => Value: ${x._2} ###"))
        )
        // commit
        dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }


    ssc.start

    ssc.awaitTermination()

  }
}
