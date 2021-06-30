package book.definitiv_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object chapter5_2_structured_api_operation {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Definitive Guide6")
      .master("local[*]")
      .getOrCreate()


    val df = spark.read
      .format("json")
      .load("data/definitive_guide_book/flight-data/json/*.json")

    println("raw data")
    df.printSchema()
    df.show()

    println("rename columns")
    val dfRenamedCols = df
      .withColumnRenamed("DEST_COUNTRY_NAME", "dest")
      .withColumnRenamed("ORIGIN_COUNTRY_NAME", "origin")

    dfRenamedCols.show()


    println("reserved characters and keywords")

    val dfWithLongColName1 = df.withColumn(
      "This is Long Column-Name",
      expr("ORIGIN_COUNTRY_NAME")
    )

    dfWithLongColName1.show()

    val dfWithLongColName2 = df.selectExpr(
      "`ORIGIN_COUNTRY_NAME` as `new origin`",
      "`count`"
    )

    dfWithLongColName2.show(2)

    println("remove columns")

    val dfRemovedCol = dfWithLongColName1
      .drop("This is Long Column-Name", "DEST_COUNTRY_NAME")

    dfRemovedCol.show()

    println("filtering rows")
    df.filter(col("count")< 2).show(2)
    df.where("count < 2").show(2)

    df
      .filter(col("count")<2)
      .filter(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)

    df
      .where("count < 2")
      .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)

    println("unique rows")
    println("Total: " + df.count())
    println("Unique: " + df.distinct().count())


    println("Random Samples")

    val seed = 5
    val withReplacement = false
    val fraction = 0.1

    df.sample(withReplacement, fraction, seed).show()
    println("Sample Size: "+df.sample(withReplacement, fraction, seed).count())


    println("Random Split (Mostly used to create the test and train dataset)")

    val dataFrame = df.randomSplit(Array(0.20, 0.70, 0.10), seed)
    val split1 = dataFrame(0)
    split1.show(3)
    println(split1.count())

    val split2 = dataFrame(1)
    split2.show(3)
    println(split2.count())

    val split3 = dataFrame(2)
    split3.show(3)
    println(split3.count())

    println("Union (Concatenating nad Appending Rows)")

    val schema = df.schema
    val newRows = Seq(
      Row("Germany", "Cameroon", 5L),
      Row("Nippon", "Preußen", 1L)
    )

    val rdd = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(rdd, schema)

    newDF.show()

    val unionDF = df.union(newDF)

    println(df.count())
    println(unionDF.count())


    println("Sorting Rows")

    df.sort(desc("count")).show(5)
    df.orderBy(desc("count")).show(5)
    df.orderBy(desc("count"), col("DEST_COUNTRY_NAME")).show(5)



    spark.stop()

  }
}
