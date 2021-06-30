package book.definitive_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object chapter6_datatypes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Definitive Guide6")
      .master("local[*]")
      .getOrCreate()


    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/definitive_guide_book/retail-data/by-day/2010-12-01.csv")

    println("### raw data ###")
    df.show()
    df.printSchema()

    println("### converting to spark type ###")
    import org.apache.spark.sql.functions.lit
    df.select(lit(1), lit("one"), lit(1.25)).show(5)

    println("### boolean ###")
    import org.apache.spark.sql.functions.col
    df.where(col("InvoiceNo").equalTo(536367))
      .select("InvoiceNo", "Description")
      .show(5, false)

    df.where(col("InvoiceNo") === 536367)
      .select("InvoiceNo", "Description")
      .show(5, false)


    val priceFilter = col("UnitPrice") > 700
    val descriptionFilter = col("Description").contains("POSTAGE")
    val stockFilter = col("StockCode").isin("DOT")

    df.where(stockFilter).where(priceFilter or descriptionFilter).show()


    import org.apache.spark.sql.functions.not
    val unitPriceFilter = not(col("UnitPrice").leq(250))
    df.withColumn("isExpensive", unitPriceFilter).filter("isExpensive").show()

    import org.apache.spark.sql.functions.expr
    val PriceExpr = expr("NOT UnitPrice <= 250")
    df.withColumn("isExpensive", PriceExpr).filter("isExpensive").show()



    spark.stop()
  }
}
