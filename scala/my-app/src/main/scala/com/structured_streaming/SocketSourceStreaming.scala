package com.structured_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}

// nc -lk 9999

object SocketSourceStreaming {
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

    import spark.implicits._
    val rawDF = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    println("Streaming DataFrame : " + rawDF.isStreaming)

    val wordCountDF = rawDF
      .select(explode(split(col("value"), " ")).alias("words"))
      .groupBy("words")
      .count()

    val query = wordCountDF
      .writeStream
      .outputMode("update")
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()

  }

}
