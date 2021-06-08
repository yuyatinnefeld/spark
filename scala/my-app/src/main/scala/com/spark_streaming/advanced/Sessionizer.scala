package com.spark_streaming.advanced

import com.spark_streaming.Utilities.{apacheLogPattern, setupLogging}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

import java.util.regex.Matcher

/** clickstreams on sessions tied together by IP addresses.
 */
object Sessionizer {

  case class SessionData(val sessionLength: Long, var clickstream: List[String]);

  /** IP address as the key, a String as a URL (wrapped in an Option to handle exceptions), and
   * maintains state defined by our SessionData class defined above.
   */
  def trackStateFunc(batchTime: Time, ip: String, url: Option[String], state: State[SessionData]): Option[(String, SessionData)] = {

    // Extract the previous state passed in (using getOrElse to handle exceptions)
    val previousState = state.getOption.getOrElse(SessionData(0, List()))

    // Create a new state that increments the session length by one, adds this URL to the clickstream, and clamps the clickstream
    // list to 10 items
    val newState = SessionData(previousState.sessionLength + 1L, (previousState.clickstream :+ url.getOrElse("empty")).take(10))

    // Update our state with the new state.
    state.update(newState)

    Some((ip, newState))
  }

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "Sessionizer", Seconds(1))
    setupLogging()
    val pattern = apacheLogPattern()


    // session timeout value of 30 minutes.
    val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the (ip, url) we want from each log line
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
        (ip, url)
      } else {
        ("error", "error")
      }
    })

    // StateSpec to update the stateful session data
    val requestsWithState = requests.mapWithState(stateSpec)

    // take a snapshot of the current state
    val stateSnapshotStream = requestsWithState.stateSnapshots()

    // Process each RDD from each batch as it comes in
    stateSnapshotStream.foreachRDD((rdd, time) => {

      // state data as SparkSQL, but you could update some external DB in the real world

      val spark = SparkSession
        .builder()
        .appName("Sessionizer")
        .getOrCreate()

      import spark.implicits._

      // Slightly different syntax here from our earlier SparkSQL example. toDF can take a list
      // of column names, and if the number of columns matches what's in your RDD, it just works
      // without having to use an intermediate case class to define your records.
      // Our RDD contains key/value pairs of IP address to SessionData objects (the output from
      // trackStateFunc), so we first split it into 3 columns using map().
      val requestsDataFrame = rdd.map(x => (x._1, x._2.sessionLength, x._2.clickstream)).toDF("ip", "sessionLength", "clickstream")

      // Create a SQL table from this DataFrame
      requestsDataFrame.createOrReplaceTempView("sessionData")

      // Dump out the results - you can do any SQL you want here.
      val sessionsDataFrame =
        spark.sqlContext.sql("select * from sessionData")
      println(s"========= $time =========")
      sessionsDataFrame.show()

    })

    // Kick it off
    ssc.checkpoint("/Users/yuyatinnefeld/desktop/checkpoint")
    ssc.start()
    ssc.awaitTermination()


    //  Result Example

    //  ========= 1620284669000 ms =========
    //  +--------------+-------------+--------------------+
    //  |            ip|sessionLength|         clickstream|
    //  +--------------+-------------+--------------------+
    //  | 66.249.75.168|           11|[/blog/, /nationa...|
    //  |   92.63.87.89|            1|[/wp-content/plug...|
    //  |85.234.159.166|            1|     [/wp-login.php]|
    //  |  180.76.15.33|            8|[/, /, /, /, /, /...|
    //  |  180.76.15.11|            3|[/australia/?pg=1...|


  }
}
