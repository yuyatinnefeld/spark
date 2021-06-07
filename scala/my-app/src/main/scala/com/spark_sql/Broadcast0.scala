package com.spark_sql

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Broadcast0 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("City")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val peopleDF = Seq(
    ("andrea", "tokyo", 1),
    ("hana", "rom", 2),
    ("max", "berlin", 30),
    ("nina", "tokyo", 10)
  ).toDF("first_name", "city", "year")

  peopleDF.show()

  val citiesDF = Seq(
    ("berlin", "germany", 3.5),
    ("tokyo", "japan", 38.0),
    ("rom", "italy", 2.7)
  ).toDF("city", "country", "population")

  citiesDF.show()

  val mergedDF = peopleDF
    .join(broadcast(citiesDF), peopleDF("city") <=> citiesDF("city"))
    .drop(citiesDF("city"))

  mergedDF.show()

  val mergedDF2 = peopleDF
    .join(broadcast(citiesDF), Seq("city"))

  mergedDF2.show()

  val onlyTokyo = peopleDF.join(
    broadcast(citiesDF),
    Seq("city"))
    .filter($"city" === "tokyo")
    .drop("city")
    .drop("country")
    .drop("population")

  onlyTokyo.show()

}
