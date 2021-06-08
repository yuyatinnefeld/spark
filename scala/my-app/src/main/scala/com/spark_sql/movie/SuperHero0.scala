package com.spark_sql.movie

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SuperHero0 {

  case class SuperHeroNames(heroID: Int, name: String)
  case class SuperHero(value:String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMoviesNicer")
      .master("local[*]")
      .getOrCreate()

    val heroSchema = new StructType()
      .add("heroID", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val heroNameDS = spark.read
      .option("sep", " ")
      .schema(heroSchema)
      .csv("data/movie/Marvel-names.txt")
      .as[SuperHeroNames]

    val heroRelationDS = spark.read
      .text("data/movie/Marvel-graph.txt")
      .as[SuperHero]

    println("### rawDS Hero Names ###")
    println("Total rows: "+heroNameDS.count())
    heroNameDS.show(5)

    println("### rawDS Hero Relation Graph ###")
    println("Total rows: "+heroRelationDS.count())
    heroRelationDS.show(5)

    println("### Main Hero DS ###")
    val mainHeroDS = heroRelationDS
      .withColumn("heroID", split(col("value"), " ")(0))
      .select("heroID")

    println("### JOINED DS ###")
    val mainHeroName = mainHeroDS.join(heroNameDS, usingColumn = "heroID")
    mainHeroName.show(10)

    println("### Main Hero Ranking ###")
    val heroCount = mainHeroName.groupBy(("heroID")).count()
    val mainHeroRanking = heroCount.join(heroNameDS, usingColumn = "heroID")
    mainHeroRanking.orderBy(desc("count")).show(10)

    spark.stop()

  }

}
