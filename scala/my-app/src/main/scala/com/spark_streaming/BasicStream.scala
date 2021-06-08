package com.spark_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

object BasicStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val linesDStream = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = linesDStream.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

}
