package com.spark_streaming.twitter

//import org.apache.spark.streaming.twitter.TwitterUtils
import com.spark_streaming.Utilities.{setupLogging, setupTwitter}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TweetStream extends App {
  setupTwitter()
  val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
  setupLogging()
  //val tweets = TwitterUtils.createStream(ssc, None)
  //val statuses = tweets.map(status => status.getText())
  //statuses.print()
  ssc.start()
  ssc.awaitTermination()

}
