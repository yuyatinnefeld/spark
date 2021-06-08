package com.spark_sql.movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

object PopularMovie1 {

  case class MovieRating(userID:Int, movieID:Int, rating:Int, timestamp:Long)

  def loadMovieNames(): Map[Int, String] = {

    implicit val codec: Codec = Codec("ISO-8859-1") //encoding of u.item, not UTF-8

    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/movie/movie.item")
    for (l <- lines.getLines()){
      val fields = l.split('|')
      if(fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    lines.close()

    movieNames

  }
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

    val nameDict = spark.sparkContext.broadcast((loadMovieNames()))

    import spark.implicits._

    val moviesDS = spark.read
      .option("sep", "\t")
      .schema(ratingSchema)
      .csv("data/movie/rating.data")
      .as[MovieRating]

    moviesDS.show(5)

    val movieCounts = moviesDS.groupBy("movieID").count()

    // declaring an "anonymous function" in scala
    // take Int       => return String
    // (movieID:Int)  => {nameDict.value(movieID)}
    val lookupName : Int => String =
    (movieID:Int) => {nameDict.value(movieID)}

    // User-Defined-Func = udf, define new Column-based functions
    val lookupNameUDF = udf(lookupName)

    // add the movieTitle column
    val movieWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))
    val movieRanking = movieWithNames.orderBy(desc("count"))

    movieRanking.show(10,false)

    spark.stop()

  }

}
