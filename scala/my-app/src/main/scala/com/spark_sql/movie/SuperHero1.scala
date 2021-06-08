package com.spark_sql.movie

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SuperHero1 {

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

    val connections = heroRelationDS
      .withColumn("heroID", split(col("value"), " ")(0))
      .withColumn("connection_size", size(split(col("value"), " ")) - 1)
      .groupBy("heroID").agg(sum("connection_size").alias("how_many_connections"))

    println("### How Many Connections ###")
    connections.show(10)

    println("### Most Popular Hero ###")

    val mostPopular = connections
      .sort($"how_many_connections".desc)
      .first()

    val mostPopular_heroID = mostPopular(0)

    val mostPopularName = heroNameDS
      .filter($"heroID" === mostPopular_heroID)

    mostPopularName.show()

    println("### Most Obscure Hero ###")

    val minCounts = connections.agg(min("how_many_connections")).first().getLong(0)
    val minConnections = connections.filter($"how_many_connections" === minCounts)
    val minConnectionsWithName = minConnections.join(heroNameDS, usingColumn = "heroID")

    minConnectionsWithName.select("name", "how_many_connections").show()

    spark.stop()

  }

}
