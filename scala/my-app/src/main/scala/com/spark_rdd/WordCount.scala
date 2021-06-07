package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")
    val rdd = sc.textFile("data/book.txt")
    val words = rdd.flatMap(x => x.split("\\W+"))
    val lowercaseWords = words.map(x => x.toLowerCase())
    val countedWords = lowercaseWords.map(x => (x, 1))
    val aggregatedWords = countedWords.reduceByKey((x, y) => x + y)
    val finalResult = aggregatedWords.map(x => (x._2, x._1)).sortByKey().collect()

    finalResult.reverse.foreach(println)

    sc.stop()
  }
}
