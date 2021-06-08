package com.structured_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object BasicStream {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SimpleStream")
      .getOrCreate()

    println("Structured Streaming created...")

    import spark.implicits._

    val rawDF = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

    println("Streaming DataFrame : " + rawDF.isStreaming)

    val counterDF = rawDF
      .withColumn("result", col("value") + lit(1))

    val query = counterDF
      .writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()


  }

}
