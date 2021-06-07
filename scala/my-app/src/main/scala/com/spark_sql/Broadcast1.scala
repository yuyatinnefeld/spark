package com.spark_sql

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Broadcast1{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val Characters = scala.collection.mutable.Map[Int, String]()

    Characters += (
      0 -> "Hanamichi",
      1 -> "Sendo",
      2 -> "Sawakita",
      3 -> "Rukawa",
      4 -> "Gori",
      5 -> "Micchi",
      6 -> "Ryota",
      7 -> "Meganekun",
      8 -> "Uozumi",
      9 -> "Anzai",
      10 -> "Maki"
    )


    val spark = SparkSession
      .builder
      .appName("Slam Dunk")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(Characters)

    val genderDF = spark.read
      .option("multiLine", true)
      .json("data/socialmedia/slamdunk_gender.json")

    println("### gender json data ###")
    genderDF.show(15)

    println("### character table ###")
    Characters.foreach(println)

    val lookupName : Int => String =
      (id:Int) => {nameDict.value(id)}

    val lookupNameUDF = udf(lookupName)

    val slamDankWithName = genderDF.withColumn("Name", lookupNameUDF(col("userid")))

    println("### merged data table ###")

    slamDankWithName.show()
    
    spark.stop()

  }


}
