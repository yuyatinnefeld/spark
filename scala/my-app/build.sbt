ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "1.0"
ThisBuild / organization     := "com.scala.spark"
ThisBuild / organizationName := "yt"

lazy val root = (project in file("."))
  .settings(
        name := "my-app",
        libraryDependencies ++= Seq(
              "org.apache.spark" %% "spark-core" % "3.0.0",
              "org.apache.spark" %% "spark-sql" % "3.1.1",
              "org.apache.spark" %% "spark-mllib" % "3.0.0",
              "org.apache.spark" %% "spark-streaming" % "3.0.0",
              "org.apache.spark" %% "spark-avro" % "2.4.0",
              "org.twitter4j" % "twitter4j-core" % "4.0.4",
              "org.twitter4j" % "twitter4j-stream" % "4.0.4",
              "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.1.1",
              "org.apache.kafka" % "kafka-clients" % "2.7.0"
        )

  )