package book.definitiv_spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object chapter2_physical_plan {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Definitive Guide1")
      .master("local[*]")
      .getOrCreate()

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/definitive_guide_book/flight-data/csv/*.csv")

    flightData2015.show()
    flightData2015.printSchema()
    flightData2015.describe("count").show()


    println("### the physical plan of SQL and DataFrame is same ###")

    val explain1 = flightData2015.explain()
    println(explain1)

    val explain2 = flightData2015.filter(flightData2015("DEST_COUNTRY_NAME") === "United States").sort("count").explain()
    println(explain2)

    flightData2015.createOrReplaceTempView("flight_data_2015")


    val sqlWay = spark.sql("""
    SELECT DEST_COUNTRY_NAME, count(1)
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    """)

    val dfWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()

    println(sqlWay.explain())
    println(dfWay.explain())

    println("### max count of DataFrame and SQL ###")

    val maxSQL = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 5
    """)

    maxSQL.show()

    val maxDataFrame = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .agg(sum("count").as("destination_total"))
      .sort(desc("destination_total")) // or       .orderBy(desc("destination_total"))
      .limit(5)

    maxDataFrame.show()



    spark.stop()


  }

}
