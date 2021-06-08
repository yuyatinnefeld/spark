package com.spark_streaming.twitter

import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.twitter.TwitterUtils
import com.spark_streaming.Utilities.{setupLogging, setupTwitter}

object TwitterTweets3 {

  def main(args: Array[String]) {
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
    setupLogging()
    //val tweets = TwitterUtils.createStream(ssc, None)
    //val statuses = tweets.map(status => status.getText())
    //statuses.saveAsTextFiles("data/tweets/Tweets", "txt")

    ssc.checkpoint("/Users/yuyatinnefeld/desktop/projects/spark/scala/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
