package quiz

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.io.{Codec, Source}


// movie_user.user
// 1|24|M|technician|85711
// 2|53|F|other|94043
// 3|23|M|writer|32067
// 4|24|M|technician|43537


object Q4_MovieReview2 extends App{

  def loadMovieNames(): Map[Int, String] = {

    implicit val codec: Codec = Codec("ISO-8859-1") //encoding of u.item, not UTF-8

    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("data/movie/movie.item")
    for (l <- lines.getLines()) {
      val fields = l.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()
    movieNames
  }

  case class UserProperty(userID:Int, age:Int, gender:String, job:String, movieID:Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("MovieUser")
    .master("local[*]")
    .getOrCreate()

  val userSchema = new StructType()
    .add("userID", IntegerType, nullable = true)
    .add("age", IntegerType, nullable = true)
    .add("gender", StringType, nullable = true)
    .add("job", StringType, nullable = true)
    .add("movieID", IntegerType, nullable = true)

  val nameDict = spark.sparkContext.broadcast((loadMovieNames()))

  import spark.implicits._
  val userPropertyDS = spark.read
    .option("sep", "|")
    .schema(userSchema)
    .csv("data/movie/movie_user.user")
    .as[UserProperty]

  userPropertyDS.show()

  println("### what kind of user watched movies the most? ###")

  val jobCounter = userPropertyDS
    .groupBy("job")
    .agg(sum("userID").alias("total users"))
    .sort("total users")

  jobCounter.show()

  spark.stop()

}
