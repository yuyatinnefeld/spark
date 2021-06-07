package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object ECommerce {
  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val userId = fields(0).toInt
    val amount = fields(2).toFloat
    (userId, amount)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Ecommerce")

    val input = sc.textFile("data/ecommerce/customer-orders.csv")
    val rdd = input.map(parseLine)

    rdd.foreach(println)

    val totalByCustomer = rdd.reduceByKey((x, y) => x + y)
    val flipped = totalByCustomer.map(x => (x._2, x._1))
    val totalByCustomerSorted = flipped.sortByKey()
    val results = totalByCustomerSorted.collect()

    for (i <- results) {
      println("Amount: " + i._1 + " | " + "UserID: " + i._2)
    }

    sc.stop()


  }
}
