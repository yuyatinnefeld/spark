package com.spark_rdd

import org.apache.kafka.common.utils.Utils.sleep
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BasicRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[3]", "rddBasic")
    val rdd = sc.parallelize(List(1, 2, 3, 4, 6, 6, 7, 10, 10, 10))

    println("open the URL: http://localhost:4040")

    while(true){
      println("## NEVER ENDING :) ###")

      val moreThan8 = rdd.filter(_ > 8)
      rdd.foreach(println)
      moreThan8.map(x => x * 100).foreach(println)

      sleep(3000)
    }

    sc.stop()
  }

}
