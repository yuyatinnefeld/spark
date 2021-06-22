package book.definitiv_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, column, expr, lit}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object chapter5_structured_api_operation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Definitive Guide5")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load("data/definitive_guide_book/flight-data/json/*.json")

    println("raw data")
    df.show()


    println("### Schema ###")


    println("automate created schema ")
    df.printSchema()


    println("manually created schema ")

    val schema = new StructType()
      .add("DEST_COUNTRY_NAME", StringType, nullable = true)
      .add("ORIGIN_COUNTRY_NAME", StringType, nullable=true)
      .add("count", LongType, nullable = true)

    val df2 = spark.read
      .format("json")
      .schema(schema)
      .load("data/definitive_guide_book/flight-data/json/*.json")

    df2.printSchema()


    println("### Columns and Expressions ###")

    val col1 = col("someColumn")
    val col2 = column("someColumn2")

    println(col1)
    println(col2)

    val count_col = df
      .col("count")

    println(count_col)

    val expressions = expr("someColumn1")
    println(expressions)

    println("### Rows / Records ###")
    val myRow1 = Row("hello", "hi", 1)
    val myRow2 = Row("world", "!", 10)
    val myRow3 = Row("last", "one", 20)

    println(myRow1(0).asInstanceOf[String])
    println(myRow1.getString(0))


    println("### DF Creation ###")
    val myRows = Seq(
      myRow1, myRow2,myRow3
    )

    val myRDD = spark.sparkContext.parallelize(myRows)

    val mySchema = new StructType()
      .add("someCol1", StringType, nullable = true)
      .add("someCol2", StringType, nullable=true)
      .add("someCol3", IntegerType, nullable = true)

    val myDF = spark.createDataFrame(myRDD, mySchema)

    myDF.printSchema()

    myDF.show()

    val myRDD2 = spark.sparkContext.parallelize(
      Seq(("berlin", "germany", "europe"), ("tokyo", "japan", "asia"))
    )

    import spark.implicits._
    val myDF2 = myRDD2.toDF("city", "country", "continent")
    myDF2.show()


    println("### DF select and selectExpr ###")

    df2.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


    df2.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

    df2.select(col("DEST_COUNTRY_NAME").as("destination2")).show(2)

    df2.selectExpr("DEST_COUNTRY_NAME AS destination3").show(2)

    df2.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

    //use Literals as counter
    df2.select(expr("*"),lit(1).as("One")).show(5)
    df2.withColumn("numberOne", lit(1)).show(5)


    spark.stop()

  }

}
