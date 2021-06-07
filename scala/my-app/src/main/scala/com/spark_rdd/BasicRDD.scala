package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BasicRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "rddBasic")
    val aList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val input = sc.parallelize(aList)

    // ## RDD MAP FUNC ## //
    val result = input.map(x => x * x).collect()
    result.foreach(println)

    // ## RDD FILTER FUNC ## //
    val data = Seq("Project alibaba", "Gutenberg’s", "Alice’s", "Adventures", "in", "Wonderland", "Project", "Gutenberg’s", "Adventures", "in", "Wonderland", "Project", "Gutenberg’s")

    val input2 = sc.parallelize(data)
    val result2 = input2.filter(x => x.length > 9)
    result2.foreach(println)


  }

}
