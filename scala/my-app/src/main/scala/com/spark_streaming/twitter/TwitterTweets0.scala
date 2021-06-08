package com.spark_streaming.twitter

//import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterTweets0 {

  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  def setupTwitter(): Unit = {
    import scala.io.Source
    val lines = Source.fromFile("conf/twitter.txt")
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
    lines.close()
  }

  def main(args: Array[String]) {
    // setupTwitter()
    // val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    // setupLogging()
    // val tweets = TwitterUtils.createStream(ssc, None)
    // val statuses = tweets.map(status => status.getText())
    // statuses.print()
    // ssc.start()
    // ssc.awaitTermination()
  }
}
