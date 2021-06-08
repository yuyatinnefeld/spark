package quiz

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

object Q1_ECommerce extends App{

  case class Order(customerID:Int, productID:Int, amount:Float)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("ECommerce")
    .master("local[*]")
    .getOrCreate()

  val orderSchema = new StructType()
    .add("customerID", IntegerType, nullable = true)
    .add("productID", IntegerType, nullable = true)
    .add("amount", FloatType, nullable = true)

  import spark.implicits._
  val rawDS = spark.read
    .schema(orderSchema)
    .csv("data/ecommerce/customer-orders.csv")
    .as[Order]

  //MY SOLUTION
  val orderDS = rawDS
    .select("customerID", "amount")
    .groupBy("customerID")
    .sum("amount")

  val renameOrderDS = orderDS
    .withColumnRenamed("sum(amount)","Total Amount")

  val resultDS= renameOrderDS
    .withColumn("Total Amount", round(col("Total Amount"),2))
    .sort("Total Amount")

  println("The Total. Order by Customer")
  resultDS.show(resultDS.count.toInt)


  val avgOrderDS = rawDS
    .groupBy("customerID")
    .agg(round(avg("amount"),2).alias("AVG"))

  println("The Avg. Order by Customer")
  avgOrderDS.show()

  // BETTER SOLUTION
  val orderDS2 = rawDS
    .groupBy("customerID")
    .agg(round(sum("amount"),2).alias("Total Amount"))
    .sort("Total Amount")

  orderDS2.show(orderDS2.count.toInt, 10)

  spark.stop()
}
