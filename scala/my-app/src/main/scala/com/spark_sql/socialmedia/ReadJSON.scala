package com.spark_sql.socialmedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReadJSON {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SocialMedia")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val dataFrame = spark.read
      .option("multiLine", true)
      .json("data/socialmedia/character.json")

    dataFrame.show()
    dataFrame.printSchema()

    val sampleDF = dataFrame.withColumnRenamed("id", "personal_id")

    val jobDF = sampleDF.select("personal_id", "age", "jobs.job")
    jobDF.printSchema()


    val jobDF2 = jobDF
      .select($"personal_id", $"age", explode($"job").alias("job_detail"))

    jobDF2.show()
    jobDF2.printSchema()


    val finalDF = jobDF2
      .select("personal_id", "age", "job_detail.*")
      .withColumnRenamed("company", "company_name")
      .withColumnRenamed("department", "department_name")
      .withColumnRenamed("location", "city")


    finalDF.show()

    spark.stop()
  }

}
