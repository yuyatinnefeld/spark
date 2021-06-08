package com.spark_streaming.advanced

import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.spark_streaming.Utilities.{apacheLogPattern, setupLogging}

import java.util.regex.Matcher

/** Illustrates using a custom receiver to listen for Apache logs on port 7777
 * and keep track of the top URL's in the past 5 minutes.
 */
object CustomReceiverUse {

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "CustomReceiverExample", Seconds(1))
    setupLogging()
    val pattern = apacheLogPattern()


    val lines = ssc.receiverStream(new CustomReceiver("localhost", 7777))

    // Extract the request field from each log line
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)
    })

    // Extract the URL from the request
    val urls = requests.map(x => {
      val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"
    })

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    // Kick it off
    ssc.checkpoint("/Users/yuyatinnefeld/desktop/checkpoint\"")
    ssc.start()
    ssc.awaitTermination()
  }
}
