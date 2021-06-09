package com.structured_streaming.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}


object NOTWORKING_SimpleKafka {

  case class KafkaVal(key:String, value:String)

  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SimpleStream")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Structured Streaming created...")

    val bootstrap_server = "127.0.0.1:9092"
    val my_topic = "spark_kafka"

    val rawDF = spark
      .read //readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap_server)
      .option("subscribe", my_topic)
      .load()


    println("Streaming DataFrame : " + rawDF.isStreaming)

    rawDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
    //val query = rawDF
    //  .writeStream
    //  .outputMode("update")
    //  .option("truncate", false)
    //  .format("console")
    //  .start()
//
    //query.awaitTermination()

  }

}
