package com.structured_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// nc -lk 9999

object SocketSourceStreaming1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SimpleStream")
      .getOrCreate()

    val host = "127.0.0.1"
    val port = "9999"

    println("Structured Streaming created...")

    val rawDF = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    println("Streaming DataFrame : " + rawDF.isStreaming)

    val query = rawDF
      .writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()

  }

}
