package com.structured_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

// > cd data/socialmedia
// > nc -lk 9999 < fk1.csv

object CSVStream0 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SocketSource")
    .getOrCreate()

  val host = "127.0.0.1"
  val port = "9999"

  val initDF = spark
    .readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .load()

  println("Streaming DataFrame : " + initDF.isStreaming)

  //Read all the csv files written atomically in a directory
  val userSchema = new StructType()
    .add("id", "integer")
    .add("name", "string")
    .add("age", "integer")
    .add("friends", "integer")


  val csvDF = spark
    .readStream
    .option("sep", ";")
    .schema(userSchema)
    .csv("data/socialmedia/fk1.csv")


  val query = initDF
    .writeStream
    .outputMode("append")
    .option("truncate", false)
    .format("console")
    .start()

  query.awaitTermination()


}
