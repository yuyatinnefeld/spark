package book.definitiv_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object chapter4_structured_api {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Definitive Guide4")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .range(500)
      .toDF("number")


    //df.show()
    df.select(df.col("number") + 10).show()


    spark.stop()


  }

}
