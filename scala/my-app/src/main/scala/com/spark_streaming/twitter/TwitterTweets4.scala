package com.spark_streaming.twitter

import com.spark_streaming.Utilities.setupLogging
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.concurrent.atomic.AtomicLong

object TwitterTweets4 {

  def main(args: Array[String]) {
//
    // setupTwitter()
    // val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    // setupLogging()
    // val tweets = TwitterUtils.createStream(ssc, None)
    // val statuses = tweets.map(status => status.getText())
    // val lengths = statuses.map(status => status.length())
    // var totalTweets = new AtomicLong(0)
    // var totalChars = new AtomicLong(0)
//
    // lengths.foreachRDD((rdd, time) => {
    //   var count = rdd.count()
    //   if (count > 0) {
    //     totalTweets.getAndAdd(count)
    //     totalChars.getAndAdd(rdd.reduce((x, y) => x + y))
//
    //     println("Total tweets: " + totalTweets.get() +
    //       " Total characters: " + totalChars.get() +
    //       " Average: " + totalChars.get() / totalTweets.get())
    //   }
    //   if (totalTweets.get() > 500) System.exit(0)
    // })
//
    // ssc.checkpoint("/Users/yuyatinnefeld/desktop/checkpoint")
    // ssc.start()
    // ssc.awaitTermination()
  }
}
