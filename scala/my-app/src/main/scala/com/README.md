<h1 align="center">Spark Streaming</h1> <br>

## Info
- https://spark.apache.org/docs/latest/streaming-programming-guide.html
- https://medium.com/expedia-group-tech/apache-spark-structured-streaming-first-streaming-example-1-of-6-e8f3219748ef

## Spark Streaming Local Log file Analysis

### 0. check build.sbt has spark-streaming package
```scala
// ============================================================================

  "org.apache.spark" %% "spark-streaming" % "3.0.0"

```
### 1. Write Test Program like this

```scala
import org.apache.spark._
import org.apache.spark.streaming._

object BasicStream extends App {

  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent a starvation scenario.

  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val linesDStream = ssc.socketTextStream("localhost", 9999)

  // Split each line into words
  val words = linesDStream.flatMap(_.split(" "))

  // Count each word in each batch
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)

  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()

  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
}
```
### 2. run localhost999

```bash
nc -lk 9999
```
### 3. run the spark streaming program

run the program by IntelliJ

```bash
sbt
runMain com.spark_streaming.BasicStream localhost 9999
```

### 4. type some words in the terminal
```bash
nc -lk 9999
hello hello hello hello hello
```
```bash
-------------------------------------------
Time: 1619615643000 ms
-------------------------------------------
(hello,4)
```

## Spark Streaming Local Log file Analysis

### 0. create Utilities.scala
```scala
import java.util.regex.Pattern

object Utilities {
/** Makes sure only ERROR messages get logged to avoid log spam. */
def setupLogging() = {
import org.apache.log4j.{Level, Logger}
val rootLogger = Logger.getRootLogger()
rootLogger.setLevel(Level.ERROR)
}

/** Configures Twitter service credentials using twiter.txt in the main workspace directory */
def setupTwitter() = {
import scala.io.Source

    for (line <- Source.fromFile("conf/twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
}

```

### 1. create spark program

> ex . scala/com/spark_streaming/LogAnalysis

```scala
package spark_streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark_streaming.Utilities._

import java.util.regex.Matcher

object LogAnalysis {

  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    setupLogging()

    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})

    // Extract the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    ssc.checkpoint("/Users/yuyatinnefeld/desktop/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}

```

### 2. create ncat.exe and access_log.txt and run
```bash
cd data/log
touch ncat.exe
touch access_log.txt
nc -kl 9999 < access_log.txt
```

### 3. run your spark program

IntelliJ 

or

Terminal
```bash
spark-submit --class com.spark_streaming.LogAnalysis target/scala-2.12/my-app_2.12-1.0.jar
```

## Twitter Most Popular Hashtag Tags

### 0. check build.sbt has spark-streaming package and twitter packages
```scala
// ============================================================================

  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4"

```
### 1. Write Twitter API Program like this

```scala
package spark_streaming

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object PopularHashtags {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
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

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    setupTwitter()

    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(status => status.getText)

    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))

    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    sortedResults.print

    //your local directory
    ssc.checkpoint("/Users/yuyatinnefeld/desktop/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
```
### 2. create twitter Developer Account and API App

https://developer.twitter.com/

### 3. create conf/twitter.txt and save the API & AccessToken Info
consumerKey XXXX
consumerSecret YYYYY
accessToken ZZZZ
accessTokenSecret KKKKK


### 4. create checkpoint local directory 
```bash
cd desktop
mkdir checkpoint
```

### 5. update the spark twitter program
```scala
ssc.checkpoint("...desktop/checkpoint")
```


### 6. run the twitter spark program
```bash
sbt
runMain spark_streaming.PopularHashtags
```

### 7. result

```bash
-------------------------------------------
Time: 1619625201000 ms
-------------------------------------------
(#AristobuloPorSiempre,1)
(#lgbtq,1)
(#QUACKTWTSELFIEDAY,1)
(#BoicotDocufakeYSalvame,1)
(#hr,1)
(#transgender,1)
(#saveเพนกวิน,1)
(#しいなーと,1)
(#biotech,1)
...
```
