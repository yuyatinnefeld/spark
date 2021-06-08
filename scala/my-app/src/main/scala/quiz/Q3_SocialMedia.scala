package quiz

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// - who has the most friends?
// - avg friends by age group?
//   Age Group
//    A x <= 25
//    B 25 < x < 30
//    C 31 < x < 40
//    D 41 < x < 50
//    E x > 51

// data/fakefriends.csv
// id,name,age,friends
//  0,Will,33,385
object Q3_SocialMedia extends App {

  case class Person(id: Int, name:String, age:Int, friends:Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("SocialFriends")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val rawDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/socialmedia/fakefriends.csv")
    .as[Person]

  // rawDS.show()

  val whoHasManyFriends = rawDS
    .orderBy(desc("friends"))

  // println("### who has the most friends? ###")
  // whoHasManyFriends.show(20)


  println("### avg friends by age group? ###")
  val DF2 = rawDS.withColumn(
    "age_group", when(col("age") < 20,"18-20")
      .when(col("age") >= 20 and col("age") < 25,"20-25")
      .when(col("age") >= 25 and col("age") < 30,"25-30")
      .when(col("age") >= 30 and col("age") < 35,"30-35")
      .when(col("age") >= 35 and col("age") < 40,"35-40")
      .when(col("age") >= 40 and col("age") < 45,"40-45")
      .when(col("age") >= 45 and col("age") < 50,"45-50")
      .when(col("age") >= 50 and col("age") < 55,"50-55")
      .when(col("age") >= 55 and col("age") < 60,"55-60")
      .otherwise(" > 60"))
  // DF2.show()

  val friendsByAgeGroup = DF2
    .select("age_group", "friends")
    .groupBy("age_group")
    .agg(round(avg("friends"),2).alias("avg friends"))
    .sort("avg friends")

  friendsByAgeGroup.show()


}
