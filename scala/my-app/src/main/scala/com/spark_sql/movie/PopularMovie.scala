package com.spark_sql.movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object PopularMovie {

  case class MovieRating(userID:Int, movieID:Int, rating:Int, timestamp:Long)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val ratingSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    val moviesDS = spark.read
      .option("sep", "\t")
      .schema(ratingSchema)
      .csv("data/movie/rating.data")
      .as[MovieRating]

    moviesDS.show(5)

    val newDS = moviesDS
      .groupBy("movieID")
      .agg(round(avg("rating"),2).alias("avg rating"))

    println("### Avg Rating by Movie ###")
    newDS.show(10)

    spark.stop()
  }

}
