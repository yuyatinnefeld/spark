package book.advanced_analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BasicDS {

  case class dataName(id_1: Int, id_2: Int, cmp_fname_c1:Option[Double], cmp_fname_c2:Option[Double],
                      cmp_lname_c1:Option[Double], cmp_lname_c2: Option[Double], cmp_sex:Option[Int], cmp_bd:Option[Int],
                      cmp_bm:Option[Int], cmp_by:Option[Int], cmp_plz:Option[Int], is_match:Boolean)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Basic")
      .getOrCreate()

    import spark.implicits._

    val dataSet = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "?")
      .csv("data/advanced_analytics_book")
      .as[dataName]

    //dataSet.show()
    println("total Records: "+dataSet.count())

    dataSet
      .groupBy("is_match")
      .count()
      .orderBy($"count".desc)
      .show()

    val summary = dataSet.describe()
    summary.select("summary","cmp_sex", "cmp_fname_c1", "cmp_fname_c2").show()


    val matches = dataSet.where("is_match = true")
    val matchSummary = matches.describe()

    val misses = dataSet.filter($"is_match === false")
    val missSummary = misses.describe()



    spark.stop()

  }
}
