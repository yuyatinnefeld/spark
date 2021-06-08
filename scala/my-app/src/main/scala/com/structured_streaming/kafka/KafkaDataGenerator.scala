package com.structured_streaming.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}

import java.time.LocalDateTime
import java.util.Properties

object KafkaDataGenerator extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  def PropsSetup(host: String): Properties ={
    val props = new Properties()
    props.put("bootstrap.servers", host)
    return props
  }


  def ProducerKafka(props: Properties, topic: String) = {

    println("Created Producer..")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    //val record = new ProducerRecord[String, String](topic, "key", "value")

    while(true){
      Thread.sleep(10000);
      val generated_data = scala.util.Random.nextInt(10000).toString
      val timestamp = LocalDateTime.now()
      println(s"data send: $timestamp - $generated_data")
      val record = new ProducerRecord[String, String](topic, "key", generated_data)
      producer.send(record)
    }
    producer.close()
  }

  println("Starting Kafka Data Generator..")

  Thread.sleep(5000)
  val props = PropsSetup("localhost:9092")
  ProducerKafka(props,"spark_kafka")

}
