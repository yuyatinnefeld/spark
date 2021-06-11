package com.spark_rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Accumulator_BroadcastVar {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "rddBasic")
    val rdd = sc.parallelize(List(5, 10, 15, 20))


    println("### Accumulator Example ###")

    val accum = sc.longAccumulator("SumAccumulator")

    rdd.foreach(x => accum.add(x))
    println(accum.value)

    print("### Broadcast Example ###")

    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )

    val rdd2 = sc.parallelize(data)

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    val broadcastStates = sc.broadcast(states)
    val broadcastCountries = sc.broadcast(countries)

    val rddNew = rdd2.map( x => {
      val country = x._3
      val state = x._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (x._1, x._2, fullCountry, fullState)
    })

    rddNew.foreach(println)

  }

}
