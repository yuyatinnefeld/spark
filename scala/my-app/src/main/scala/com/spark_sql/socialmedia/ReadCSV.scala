package com.spark_sql.socialmedia
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, round}

object ReadCSV {

  case class Person(id: Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SocialMedia")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val dataSet = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/socialmedia/fakefriends.csv")
      .as[Person]

    dataSet.show()
    dataSet.printSchema()

    val dataSet_without_header = spark.read
      .option("header", "false")
      .csv("data/socialmedia/fakefriends-noheader.csv")
      .toDF("id","name","age","friends")

    dataSet_without_header.show()
    dataSet_without_header.printSchema()

    //Avg friends by Age

    println("## Solution1: sparkSQL - old fashion.. not good ##")

    dataSet.createOrReplaceTempView("people")

    val avgFriendsByAge = spark.sql("SELECT age, AVG(friends) as avg_friends FROM people GROUP BY age").collect()
    avgFriendsByAge.foreach(println)

    println("## Solution2: DataSet - it's ok but... ##")
    val avgFriendsByAge2 = dataSet
      .select("age", "friends")
      .groupBy("age")
      .avg("friends").sort("age")

    avgFriendsByAge2.show()

    println("## Solution3: DataSet + Roundup + Rename - that's fine ##")
    val avgFriendsByAge3 = dataSet
      .select("age", "friends")
      .groupBy("age")
      .agg(
        round(avg("friends"),2).alias("avg-friends")
      )
      .sort("age")


    avgFriendsByAge3.show()


    spark.stop()

  }
}
