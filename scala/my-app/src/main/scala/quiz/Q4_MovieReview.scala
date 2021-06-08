package quiz

import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.io.{Codec, Source}

//  - top 10 ranking the most watched movies?
//  - how many movies with rating 3,4,5?
//  - what kind of user watched movies the most?
//  - what kind of movies like programmer, engineer, scientist?
//      =>data/movie/movie.item
//      =>data/movie/movie.data
//      =>data/movie/movie_user.user

// u.item
// 1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0

// u.data
// 0	50	5	881250949
// 0	172	5	881250949
// 0	133	1	881250949



object Q4_MovieReview extends App{

  case class Movies(userID: Int, movieID:Int, rating: Int, timestamp: Long)

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

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("PopularMoviesNicer")
    .master("local[*]")
    .getOrCreate()

  // Broadcast
  val nameDict = spark.sparkContext.broadcast((loadMovieNames()))

  val moviesSchema = new StructType()
    .add("userID", IntegerType, nullable = true)
    .add("movieID", IntegerType, nullable = true)
    .add("rating", IntegerType, nullable = true)
    .add("timestamp", LongType, nullable = true)

  import spark.implicits._
  val moviesDS = spark.read
    .option("sep", "\t")
    .schema(moviesSchema)
    .csv("data/movie/movie.data")
    .as[Movies]

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

  print("### Top Movie Ranking ###")
  movieRanking.show(movieRanking.count.toInt, truncate = 20)


  println("### Only Rating 5 Movies ###")

  val onlyRating5 = moviesDS
    .filter($"rating" === 5)
    .select("movieID", "rating")

  //onlyRating5.show()
  val movieWithNames2 = onlyRating5.withColumn("movieTitle", lookupNameUDF(col("movieID")))
  movieWithNames2.show(truncate = false)


  spark.stop()

}
