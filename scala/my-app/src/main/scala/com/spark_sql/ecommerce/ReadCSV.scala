package com.spark_sql.ecommerce

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StringType, StructType}

object ReadCSV {

  case class Order(customerID: String, itemID:String, amount:Float)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ECommerce")
      .master("local[*]")
      .getOrCreate()

    val orderSchema = new StructType()
      .add("customerID", StringType, nullable = true)
      .add("itemID", StringType, nullable = true)
      .add("amount", FloatType, nullable =true)

    import spark.implicits._
    val dataSet = spark.read
      .schema(orderSchema)
      .csv("data/ecommerce/customer-orders.csv")
      .as[Order]

    dataSet.printSchema()
    dataSet.show(5)
    dataSet.describe("customerID", "itemID", "amount").show()

    val totalByCustomer = dataSet
      .groupBy("customerID")
      .agg(round(sum("amount"),2).alias("total_amount"))

    totalByCustomer.show()

    spark.stop()


  }

}
