package book.advanced_analytics

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BasicRDD {

  case class dataName(id_1: Int, id_2: Int, cmp_fname_c1:Option[Double], cmp_fname_c2:Option[Double],
                      cmp_lname_c1:Option[Double], cmp_lname_c2: Option[Double], cmp_sex:Option[Int], cmp_bd:Option[Int],
                      cmp_bm:Option[Int], cmp_by:Option[Int], cmp_plz:Option[Int], is_match:Boolean)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[3]", "Basic")
    val rdd = sc.textFile("data/advanced_analytics_book")

    println("columns: " + rdd.first())

    val head = rdd.take(10)
    head.foreach(println)

    println("Records: " + head.length)

    def isHeader(line:String):Boolean = line.contains("id_1")

    val noheaderRDD = head.filter(!isHeader(_))
    println("Records: " + noheaderRDD.length)

    println("### RDD -> DF ###")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Basic")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "?")
      .csv("data/advanced_analytics_book")

    df.printSchema()


    println("### RDD -> DS ###")

    val dataSet = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "?")
      .csv("data/advanced_analytics_book")
      .as[dataName]

    dataSet.show()
    println("total Records: "+dataSet.count())

    spark.stop()
    sc.stop()


  }

}
