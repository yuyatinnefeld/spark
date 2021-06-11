package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BasicTransformationRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "rddBasic")
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    println("## RDD Map ###")
    val result1 = rdd.map(x => x * x)
    result1.foreach(println)

    println("## RDD FlatMap ###")
    val result2 = rdd.flatMap(x => x.to(2))
    result2.foreach(println)

    println("## RDD Filter ###")
    val rdd2 = sc.parallelize(Seq("Banana", "Alice’s", "ALICE’S", "Alice’s", "thunderbird", "in", "Wonderland", "b", "b", "b"))
    val result3 = rdd2.filter(x => x.length > 9)
    result3.foreach(println)

    println("## RDD Distinct ###")
    val result4 = rdd2.distinct()
    result4.foreach(println)

    println("## RDD UNION ###")
    val rdd3 = sc.parallelize(Seq("OKONOMIYAKI", "SOURCE","ALICE’S"))
    val result5 = rdd2.union(rdd3)
    result5.foreach(println)

    println("## RDD Intersection ###")
    val result6 = rdd2.intersection(rdd3)
    result6.foreach(println)

    println("## RDD KEY VALUE TRANSFORMATION ###")
    val x= sc.parallelize(Seq(("math", 55),("math", 40),("math", 40),("english", 58),("english", 58),("science", 58),("science", 54)))

    println("## RDD keys ###")
    val k = x.keys
    k.foreach(println)

    println("## RDD values ###")
    val v = x.values
    v.foreach(println)

    println("## RDD reduceByKey ###")
    val summary = x.reduceByKey((x,y) => x + y)
    summary.foreach(println)

    println("## RDD groupByKey ###")
    val gbk = x.groupByKey()
    gbk.foreach(println)

    println("## RDD mapValues ###")
    val avg_score = 50
    val mv = x.mapValues(x => x - avg_score)
    mv.foreach(println)

    val counter = x.mapValues(x => (x, 1))
    counter.foreach(println)


    println("## RDD flatMapValues ###")
    val fmv = x.flatMapValues(x => (x to 60))
    fmv.foreach(println)

    println("## RDD sortByKey ###")
    val sbk = x.sortByKey()
    sbk.foreach(println)

    println("## RDD Join ###")
    val y = sc.parallelize(Seq(("math", 60),("math", 65),("science", 61),("science", 62),("history", 63),("history", 64)))
    val z = x.join(y)
    z.foreach(println)

    println("## RDD rightOuterJoin ###")
    val roj = x.rightOuterJoin(y)
    roj.foreach(println)

    println("## RDD leftOuterJoin ###")
    val loj = x.leftOuterJoin(y)
    loj.foreach(println)

    println("## RDD filter + case ###")
    val moreThan55_v1 = x.filter({case (key, value) => value > 55})
    moreThan55_v1.foreach(println)
    println("simple version")
    val moreThan55_v2 = x.filter(_._2 > 55)
    moreThan55_v2.foreach(println)

    println("## RDD mapValues + reduceByKey  ###")
    val score = x.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1,x._2 + y._2))
    score.foreach(println)



  }

}
