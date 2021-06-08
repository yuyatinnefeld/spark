package com.structured_streaming.loganalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}

object LogfileStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .getOrCreate()

    val accessLines = spark.readStream.text("data/log")

    //  Log File Data Structure
    //  Host + Time + General + Status + ContentSize

    //  Log File Data Example
    //  54.165.199.171 - - [29/Nov/2015:04:33:37 +0000] "GET /houston-headlines/ HTTP/1.0" 200 33415 "-" "-"
    //  54.165.199.171 - - [29/Nov/2015:04:33:07 +0000] "GET /science/ HTTP/1.0" 200 44758 "-" "-"
    //  54.165.199.171 - - [29/Nov/2015:04:33:42 +0000] "GET /philadelphia-headlines/ HTTP/1.0" 200 36267 "-" "-"
    //  54.165.199.171 - - [29/Nov/2015:04:33:47 +0000] "GET /houston-sports/ HTTP/1.0" 200 29133 "-" "-"
    //  54.165.199.171 - - [29/Nov/2015:04:33:17 +0000] "GET /seattle-sports/ HTTP/1.0" 200 36844 "-" "-"
    //  54.165.199.171 - - [29/Nov/2015:04:33:57 +0000] "GET /los-angeles-sports/ HTTP/1.0" 200 34318 "-" "-"


    // Regular expressions to extract pieces of Apache access log lines
    val contentSizeExp = "\\s(\\d+)$"
    val statusExp = "\\s(\\d{3})\\s"
    val generalExp = "\"(\\S+)\\s(\\S+)\\s*(\\S*)\""
    val timeExp = "\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]"
    val hostExp = "(^\\S+\\.[\\S+\\.]+\\S+)\\s"

    // Apply these regular expressions to create structure from the unstructured text
    val logsDF = accessLines.select(regexp_extract(col("value"), hostExp, 1).alias("host"),
      regexp_extract(col("value"), timeExp, 1).alias("timestamp"),
      regexp_extract(col("value"), generalExp, 1).alias("method"),
      regexp_extract(col("value"), generalExp, 2).alias("endpoint"),
      regexp_extract(col("value"), generalExp, 3).alias("protocol"),
      regexp_extract(col("value"), statusExp, 1).cast("Integer").alias("status"),
      regexp_extract(col("value"), contentSizeExp, 1).cast("Integer").alias("content_size"))

    val statusCountsDF = logsDF.groupBy("status").count()
    val query = statusCountsDF
      .writeStream.outputMode("complete")
      .format("console")
      .queryName("counts")
      .start()

    query.awaitTermination()
    spark.stop()
  }

}
