package book.definitiv_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}

object chapter3_structured_streaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Definitive Guide2")
      .master("local[*]")
      .getOrCreate()

    val staticDataFrame = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/definitive_guide_book/retail-data/by-day/*.csv")

    val staticSchema = staticDataFrame.schema

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("sep", ",")
      .option("maxFilesPerTrigger", 1) // read files more streaming nearly
      .csv("data/definitive_guide_book/retail-data/by-day/*.csv")


    println("isStreaming ? : "+streamingDataFrame.isStreaming)


    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")

    println("structured streaming start ...")

    purchaseByCustomerPerHour.writeStream
      .format("console") // show the result in the console
      .queryName("customer_purchases") // the name of the in-memory table
      .outputMode("complete") // complete = all the counts should be in the table
      .start()
      .awaitTermination()


    spark.stop()


  }

}
