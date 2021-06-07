package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingCounter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RatingCounter")
    val lines = sc.textFile("data/movie/rating.data")

    println("### extract the column2 ###")
    val ratings = {
      lines.map(x => x.split("\t")(2))
    }
    ratings.foreach(print)

    println("### counted the values ###")
    val results = ratings.countByValue()
    results.foreach(println)

    println("### sorted the values ###")
    val sortedResult = results.toSeq.sortBy(_._1) //toSeq=sequence from scala map
    sortedResult.foreach(println)

    sc.stop()


  }

}
