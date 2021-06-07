package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RDDtoDataFrame {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataFrameConvert")
      .getOrCreate()

    val sampleData = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(sampleData)


    import spark.implicits._
    val DataFrame = rdd.toDF("language","users_count")
    DataFrame.show()



    val rdd2 = spark.sparkContext.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))


    val DataFrame2 = rdd2.toDF("text")
    DataFrame2.show(truncate = false)


    spark.stop()
  }

}
