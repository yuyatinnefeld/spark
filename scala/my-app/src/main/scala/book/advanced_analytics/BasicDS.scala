package book.advanced_analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    println("### raw data ###")
    dataSet.show()

    dataSet
      .groupBy("is_match")
      .count()
      .orderBy($"count".desc)
      .show()

    println("### process1: summarized ###")
    val summary = dataSet.describe()
    summary.select("summary","cmp_sex", "cmp_fname_c1", "cmp_fname_c2").show()


    val matches = dataSet.filter($"is_match" === true) // or dataSet.where("is_match = true")
    val matchSummary = matches.describe()

    val misses = dataSet.filter($"is_match" === false)
    val missSummary = misses.describe()

    println("### process2: wide form > long form transform ###")

    def pivotSummary(desc: DataFrame): DataFrame = {
      val schema = desc.schema
      import desc.sparkSession.implicits._

      val longForm = desc.flatMap(row =>{
        val metric = row.getString(0)
        (1 until row.size).map(i => {
          (metric, schema(i).name, row.getString(i).toDouble)
        })
      })

      val longDF = longForm.toDF("metric", "field", "value")

      val pivot_values = Seq("count", "mean", "stddev", "min", "max")

      val pivotedDF = longDF
        .groupBy("field")
        .pivot("metric", pivot_values)
        .avg()
        .sort("field")

      pivotedDF
    }



    println("### process3: create a pivot table ###")

    val matchSummaryT = pivotSummary(matchSummary)
    val missSummaryT = pivotSummary(missSummary)

    matchSummaryT.show()
    missSummaryT.show()



    spark.stop()

  }
}
