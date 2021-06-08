package quiz

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

object Q2_ECommerce extends App{

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

  val mostBuyingItem = rawDS
    .groupBy("productID")
    .agg(round(sum("amount"),2).alias("Total Amount"))
    .sort("Total Amount")


  mostBuyingItem.show(mostBuyingItem.count.toInt, 20)

}
