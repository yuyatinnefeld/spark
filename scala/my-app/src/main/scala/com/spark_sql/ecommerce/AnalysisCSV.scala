package com.spark_sql.ecommerce

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AnalysisCSV {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ECommerceAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val productDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/ecommerce/products.csv")

    productDF.show()
    productDF.describe("price").show()

    val salesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/ecommerce/sales.csv")

    salesDF.show()
    salesDF.describe("num_pieces_sold").show()

    val cleanedSalesDF = salesDF
      .filter(col("product_id") > 0)

    val joinedDF = cleanedSalesDF.as("t")
      .join(productDF.as("p"), $"p.id" === $"t.product_id", "inner")
      .select("date","product_id", "name", "price", "num_pieces_sold")
      .withColumn("order_amount", $"price" * $"num_pieces_sold")

    println("### joined DF ###")
    joinedDF.show(10)

    println("### Top 10 Products ###")
    joinedDF
      .groupBy("product_id")
      .agg(round(sum("order_amount"), 2).alias("total amount"))
      .orderBy(col("total amount").desc) // or .sort(col("avg order amount").desc)
      .show(10)

    println("### Top 10 Sales Date ###")
    joinedDF
      .groupBy("date")
      .agg(round(sum("order_amount"),2).alias("total amount"))
      .orderBy(col("total amount").desc) // or .sort(col("avg order amount").desc)
      .show()



  }

}
