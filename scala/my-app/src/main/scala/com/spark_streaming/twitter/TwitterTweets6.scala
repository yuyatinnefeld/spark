package com.spark_streaming.twitter

import com.spark_streaming.Utilities.{setupLogging, setupTwitter}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterTweets6 extends App {

  setupTwitter()
  val ssc = new StreamingContext("local[*]", "MostPopularHashtag", Seconds(1))
  setupLogging()
  // val tweets = TwitterUtils.createStream(ssc, None)
  // val statuses = tweets.map(status => status.getText())
  // val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))
  // val hashtags = tweetWords.filter(word => word.startsWith("#"))
  // val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
  // val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1))
  // val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

  // sortedResults.print

  ssc.checkpoint("/Users/yuyatinnefeld/desktop/checkpoint")
  ssc.start()
  ssc.awaitTermination()

}
