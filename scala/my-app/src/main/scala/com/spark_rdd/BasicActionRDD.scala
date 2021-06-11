package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BasicActionRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "rddBasic")
    val rdd = sc.parallelize(List(1, 2, 3, 4, 6, 6, 7, 10, 10, 10))

    println("## RDD Collect ###")
    val greaterThan5 = rdd.filter(_ > 5)
    greaterThan5.collect()
    greaterThan5.foreach(println)

    println("## RDD Count ###")
    println(rdd.count())

    println("## RDD countByValue ###")
    val count = rdd.countByValue()
    count.foreach(println)

    println("## RDD take ###")
    val take = rdd.take(3)
    take.foreach(println)

    println("## RDD top ###")
    val t = rdd.top(5)
    t.foreach(println)

    println("## RDD takeOrdered ###")
    val tob = rdd.takeOrdered(2)
    tob.foreach(println)

    println("## RDD takeSample ###")
    val sample = rdd.takeSample(false, 1)
    sample.foreach(println)

    println("## RDD reduce = sum ###")
    val rd = rdd.reduce((x,y) => (x + y))
    println(rd)

    println("## RDD KEY VALUE ACTION ###")
    val x= sc.parallelize(Seq(("math", 55),("math", 40),("math", 40),("english", 58),("english", 58),("science", 58),("science", 54)))

    println("## RDD countByKey ###")
    val cbK = x.countByKey()
    cbK.foreach(println)

    println("## RDD countByValue ###")
    val cbV = x.countByValue()
    cbV.foreach(println)

    println("## RDD collectAsMap ###")
    val caM = x.collectAsMap()
    caM.foreach(println)

    println("## RDD lookup ###")
    val lk = x.lookup("math")
    lk.foreach(println)


  }

}
