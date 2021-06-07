package com.spark_sql.socialmedia


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReadAVRO {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SocialMedia")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._
    val dataFrame = spark.read
      .format("avro")
      .load("data/socialmedia/users.avro")

    println("### import AVRO as DataFrame ###")
    dataFrame.show()


    println("### save dataFrame in AVRO ###")

    val data = Seq(("James ","","Smith",2018,1,"M",3000),
      ("Michael ","Rose","",2010,3,"M",4000),
      ("Robert ","","Williams",2010,3,"M",4000),
      ("Maria ","Anne","Jones",2005,5,"F",4000),
      ("Jen","Mary","Brown",2010,7,"",-1)
    )

    val columns = Seq("firstname", "middlename", "lastname", "dob_year", "dob_month", "gender", "salary")

    val newDataFrame = data.toDF(columns:_*)

    newDataFrame.write
      .format("avro")
      .save("data/socialmedia/person2.avro")


    val readAvroDS = spark.read
      .format("avro")
      .load("data/socialmedia/person2.avro")

    readAvroDS.show()

    println("### save dataFrame in AVRO Disk Partition ###")

    newDataFrame.write.partitionBy("dob_year","dob_month")
      .format("avro")
      .save("data/socialmedia/person_partition.avro")

    val readAvroDS2 = spark.read
      .format("avro")
      .load("data/socialmedia/person_partition.avro")
      .where(col("dob_year") === 2010)

    readAvroDS2.show()

    spark.stop()
  }


}
