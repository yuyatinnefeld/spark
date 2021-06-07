<h1 align="center">Scala Spark Quick Start </h1> <br>

## Info
- https://www.scala-sbt.org
- https://www.scala-sbt.org/1.x/docs/sbt-new-and-Templates.html

## Setup
### 0. Install SDKs, Build Tool & Editor

Intellij IDEA (Editor)
```bash
brew search intellij
brew install --cask intellij-idea-ce
```
Java SDK
```bash
https://www.oracle.com/de/java/technologies/javase-jdk15-downloads.html
```

Spark & Scala SDK
```bash
brew install scala
brew install apache-spark
```

SBT (Package Manager/ Build Tool)
```bash
brew install sbt
```

### 1. Testing
#### scala program
```bash
touch test.scala
nano test.scala
```
```scala
object test extends App{
print("hello just test")
}
```
```bash
scala s.scala
```
#### spark program
```bash
touch README.md
nano README.md
```
write something with the word spark
```bash
spark is a nice framework for DE & DS. I love spark.
```

```bash
Spark-shell
```

```bash
val textFile = spark.read.textFile("README.md")
textFile.first()
textFile.filter(line => line.contains("spark")).count() 
```

### 2. Create Project & App

create a new project
```bash
mkdir new_project
cd new_project
```

create a new app with hello-world sbt template
```bash
sbt new scala/hello-world.g8

```
app name = my-app
```bash
...
name [hello]: my-app
```

you can see the following project structure

```bash
- my-app
    - project
        - build.properties
    - src
        - main
            - scala
                - Main.scala
    - build.sbt
```

### 2. Running the project
```bash
cd myapp
```
```bash
sbt
```
```bash
~run
```
### 2. Open IntelliJ

```bash
cd myapp
idea .
```

### 3. Update build.sbt
close & open myapp (i.e. install spark packages)
```scala
// ============================================================================
import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "1.0"
ThisBuild / organization     := "com.scala.spark"
ThisBuild / organizationName := "yt"

lazy val root = (project in file("."))
  .settings(
    name := "my-app",
    libraryDependencies += scalaTest % Test,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.0.0",
      "org.apache.spark" %% "spark-sql" % "3.1.1",
      "org.apache.spark" %% "spark-mllib" % "3.0.0",
      "org.apache.spark" %% "spark-streaming" % "3.0.0",
      "org.twitter4j" % "twitter4j-core" % "4.0.4",
      "org.twitter4j" % "twitter4j-stream" % "4.0.4",
      "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.1.1",
      "org.apache.kafka" % "kafka-clients" % "2.7.0"
    )

  )

```
### 4. Run spark program
1. restart sbt
```bash
sbt reload
```

2. create input file
```bash
mkdir data
touch data/book.txt
```
3. create spark program
```bash
mkdir src/main/scala/com/spark
touch src/main/scala/com/spark/mySparkTest.scala
```
4. update spark program
```scala
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object mySparkTest extends App{

  case class Book(value:String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Solution 1 with DataFrame //
  val spark = SparkSession
    .builder
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val input = spark.read.text("data/book.txt").as[Book]

  val words = input
    .select(explode(split($"value", "\\W+")).alias("word"))
    .filter($"word" =!= "")

  val lowerCaseWords = words.select(lower($"word").alias("word"))
  val wordCounts = lowerCaseWords.groupBy("word").count()
  val wordCountsSorted = wordCounts.sort("count")

  println("### DataFrame ###")
  wordCountsSorted.show(wordCountsSorted.count.toInt)

  // Solution 2 with RDD //
  val bookRDD = spark.sparkContext.textFile("data/book.txt")
  val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
  val wordDS = wordsRDD.toDS() // to DataSet

  //same way like before
  val lowercaseWordsDS = wordDS.select(lower($"value").alias("word"))
  val wordCountDS = lowercaseWordsDS.groupBy("word").count
  val wordCountsSortedDS = wordCountDS.sort("count")

  println("### RDD ###")
  wordCountsSortedDS.show(wordCountsSorted.count.toInt)
  
}
```

5. run by IntelliJ

6. run in Terminal

```bash
spark-submit --class <package_path.scala_program> <jar_file_path>
```

```bash
targetspark-submit --class com.spark.mySparkTest target/scala-2.12/my-app_2.12-1.0.jar
```

run application locally on 8 cores

```bash
spark-submit --master local[8] --class com.spark.mySparkTest target/scala-2.12/my-app_2.12-1.0.jar
```

run on a Spark standalone cluster in client deploy mode
```bash
spark-submit \
--class com.spark.mySparkTest \
--master spark://207.184.161.138:7077 \
--executor-memory 20G \
--total-executor-cores 100 \
target/scala-2.12/my-app_2.12-1.0.jar \
1000
```