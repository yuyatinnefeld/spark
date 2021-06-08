package com.spark_streaming.twitter

//import org.apache.spark.streaming.twitter.TwitterUtils
import com.spark_streaming.Utilities.{setupLogging, setupTwitter}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterTweets5 extends App {

  setupTwitter()
  val ssc = new StreamingContext("local[*]", "MaxTweetLength", Seconds(1))
  setupLogging()
  // val tweets = TwitterUtils.createStream(ssc, None)
  // val statuses = tweets.map(status => status.getText())
  // val lengths = statuses.map(status => status.length())
  // var maxlengthsTweet: Long = 0
  // var totalTweets: Long = 0


  // lengths.foreachRDD((rdd, time) => {
  //   var count = rdd.count()
  //   totalTweets += 100
//
  //   if (count > 0) {
  //     if (maxlengthsTweet < count) maxlengthsTweet = count
  //   }
  //   println("max: " + maxlengthsTweet)
  //   if (totalTweets > 1000) System.exit(0)
//
  // })

  ssc.checkpoint("/Users/yuyatinnefeld/desktop/checkpoint")
  ssc.start()
  ssc.awaitTermination()

}
