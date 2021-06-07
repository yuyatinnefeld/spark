package com.spark_sql.log

import org.apache.log4j._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object JSONLogAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LogAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val dataFrame = spark.read
      .format("json")
      .load("data/logs/utlization.json")

    val newNames = Seq("cpu", "datetime", "memory", "server", "sessions")
    val renamedDF = dataFrame.toDF(newNames: _*)

    renamedDF.show(10)

    println("only server id = 120")

    renamedDF
      .filter("server == 120")
      .show(10)


    println("Avg Performance by Server")

    renamedDF
      .select("server", "sessions")
      .groupBy("server")
      .agg(round(avg("sessions"),2).alias("avg sessions"))
      .show()

    renamedDF
      .select("server", "cpu")
      .groupBy("server")
      .agg(round(avg("cpu"),2).alias("avg cpu"))
      .show()

    renamedDF
      .select("server", "memory")
      .groupBy("server")
      .agg(round(avg("memory"),2).alias("avg memory"))
      .show()

    println("correlation: cup + memory")
    println(renamedDF.stat.corr("cpu", "memory"))

    println("correlation: session + memory")
    println(renamedDF.stat.corr("sessions", "memory"))


    println("total number")
    println(renamedDF.count())

    println("sampling 50 %")
    val sampledDF = renamedDF.sample(fraction = 0.5, withReplacement=false)
    println(sampledDF.count())


    sampledDF.createOrReplaceTempView("utilization")

    spark.sql("SELECT * FROM utilization").show()

    spark
      .sql("SELECT min(cpu), max(cpu), stddev(cpu) FROM utilization")
      .show()

    spark
      .sql("SELECT server, min(cpu), max(cpu), stddev(cpu) " +
        "FROM utilization " +
        "GROUP BY server")
      .show()

    spark.sql("SELECT count(*), FLOOR(cpu*100/10) bucket " +
      "FROM utilization " +
      "GROUP BY bucket " +
      "ORDER BY bucket").show()


    spark.stop()
  }

}
