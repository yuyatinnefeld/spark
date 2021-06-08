package com.structured_streaming.loganalysis

import com.spark_streaming.Utilities.apacheLogPattern
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}
import java.util.regex.Pattern

object LogfileStream1 {

  case class LogEntry(ip:String, client:String, user:String, dateTime:String, request:String, status:String, bytes:String, referer:String, agent:String)
  val logPattern = apacheLogPattern()
  val datePattern = Pattern.compile("\\[(.*?) .+]")


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/Users/yuyatinnefeld/desktop/projects/spark/scala/checkpoint")
      .getOrCreate()

    val rawData = spark.readStream.text("data/log")

    import spark.implicits._
    val structuredData = rawData
      .flatMap(parseLog)
      .select("status", "dateTime")

    val windowed = structuredData
      .groupBy($"status", window($"dateTime", "1 hour")).count()
      .orderBy("window")

    val query = windowed
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

    spark.stop()

  }


  // Function to convert Apache log times to what Spark/SQL expects
  def parseDateField(field: String): Option[String] = {

    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = (dateFormat.parse(dateString))
      val timestamp = new java.sql.Timestamp(date.getTime());
      return Option(timestamp.toString())
    } else {
      None
    }
  }

  // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
  def parseLog(x:Row) : Option[LogEntry] = {

    val matcher:Matcher = logPattern.matcher(x.getString(0));
    if (matcher.matches()) {
      val timeString = matcher.group(4)
      return Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      return None
    }
  }


}
