package com.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MySparkTest extends App{

  case class Book(value:String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Solution 1 with DataFrame //
  val spark = SparkSession
    .builder
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val input = spark.read.text("data/book.txt").as[Book]

  val words = input
    .select(explode(split($"value", "\\W+")).alias("word"))
    .filter($"word" =!= "")

  val lowerCaseWords = words.select(lower($"word").alias("word"))
  val wordCounts = lowerCaseWords.groupBy("word").count()
  val wordCountsSorted = wordCounts.sort("count")

  println("### DataFrame ###")
  wordCountsSorted.show(wordCountsSorted.count.toInt)

  // Solution 2 with RDD //
  val bookRDD = spark.sparkContext.textFile("data/book.txt")
  val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
  val wordDS = wordsRDD.toDS() // to DataSet

  //same way like before
  val lowercaseWordsDS = wordDS.select(lower($"value").alias("word"))
  val wordCountDS = lowercaseWordsDS.groupBy("word").count
  val wordCountsSortedDS = wordCountDS.sort("count")

  println("### RDD ###")
  wordCountsSortedDS.show(wordCountsSorted.count.toInt)

  spark.stop()

}
