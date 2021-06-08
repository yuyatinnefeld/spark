package com.spark_mllib

import org.apache.log4j._
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.collection.mutable

object RecommendationMovie {

  case class MoviesNames(movieId: Int, movieTitle: String)
  case class Rating(userID: Int, movieID: Int, rating: Float)

  def getMovieName(movieNames: Array[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(_.movieId == movieId)(0)
    result.movieTitle
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MovieRecommendation")
      .master("local[*]")
      .getOrCreate()

    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Create schema when reading rating.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    val namesDF = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1") // Apply ISO-885901 charset
      .schema(moviesNamesSchema)
      .csv("data/movie/movie.item")
      .as[MoviesNames]

    println("### Movie Description ###")
    namesDF.show()

    val nameList = namesDF.collect()

    val ratingsDF = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/movie/rating.data")
      .as[Rating]

    println("### Movie Rating ###")
    ratingsDF.show()

    println("\nTraining recommendation model...")

    //Spark MlLib Feature ALS (Alternating Least Square) = Recommendation System
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)

    // Get top-10 recommendations for the user we specified
    val userID:Int = 0 //HERE COMING THE DYNAMIC USERID

    val users = Seq(userID).toDF("userID")
    val recommendations = model.recommendForUserSubset(users, 10)

    // Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")

    for (userRecs <- recommendations) {
      val myRecs = userRecs(1) // First column is userID, second is the recs
      val temp = myRecs.asInstanceOf[mutable.WrappedArray[Row]] // Tell Scala what it is
      for (rec <- temp) {
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = getMovieName(nameList, movie)
        println(movieName, rating)
      }
    }

    // Stop the session
    spark.stop()
  }

}
