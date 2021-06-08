package com.structured_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// > cd data/socialmedia
// > nc -lk 9999 < fk1.csv

object CSVStream1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .master("local")
    .appName("CSV ReadStream")
    .getOrCreate()

  val schema = StructType(
    Array(StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("friends", IntegerType))
  )

  import spark.implicits._

  //create stream for fb friends data
  val rawDF = spark.readStream
    .option("header", "true")
    .schema(schema)
    .csv("data/socialmedia/fb")

  println("Streaming DataFrame : " + rawDF.isStreaming)

  val query = rawDF
    .writeStream
    .format("console")
    .outputMode("append")
    .start()

  query.awaitTermination()


}
