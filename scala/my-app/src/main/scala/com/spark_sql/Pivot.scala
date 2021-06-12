package com.spark_sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Pivot {
  case class Play(play: String, country: String, points: Int)

  case class City(id: Int, city: String, year: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Basic")
      .getOrCreate()

    import spark.implicits._
    val visitsDF = Seq(
      City(0, "Warsaw", 2015),
      City(1, "Warsaw", 2016),
      City(3, "Boston", 2017),
      City(4, "Kyoto", 2016),
      City(5, "Kyoto", 2015),
      City(6, "Warsaw", 2016),
      City(7, "Kyoto", 2017)
    ).toDF()

    println(" PIVOT Example - City")
    visitsDF.show()

    val pivotDF = visitsDF
      .groupBy("city")
      .pivot("year", Seq("2015", "2016", "2017"))
      .count()

    pivotDF.show()

    println(pivotDF.queryExecution.logical.numberedTreeString)

    println(" PIVOT Example - Football ")
    val playsDF = Seq(
      Play("play1", "France", 3), Play("play2", "Poland", 1), Play("play3", "Germany", 8),
      Play("play4", "France", 0), Play("play4", "Italy", 5), Play("play5", "Germany", 3),
      Play("play6", "France", 3), Play("play5", "Poland", 0), Play("play3", "Italy", 1),
      Play("play1", "Poland", 0), Play("play6", "Germany", 2), Play("play2", "Germany", 1)
    ).toDF()

    playsDF.show()

    val pivot2DF = playsDF
      .groupBy("country")
      .pivot("Play")
      .sum("points")

    pivot2DF.show()

    spark.stop()
  }

}
