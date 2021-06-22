package book.definitiv_spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.ml.feature._

object chapter3_kmean_clustering {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Definitive Guide3")
      .master("local[*]")
      .getOrCreate()

    val staticDataFrame = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/definitive_guide_book/retail-data/by-day/*.csv")


    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
      .coalesce(5)


    val trainDataFrame = preppedDataFrame
      .where("InvoiceDate < '2011-07-01'")
    val testDataFrame = preppedDataFrame
      .where("InvoiceDate >= '2011-07-01'")

    println(trainDataFrame.count())
    println(testDataFrame.count())

    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")

    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

    // 3 key features = price, quantity, day of week
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")


    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))

    val fittedPipeline = transformationPipeline.fit(trainDataFrame)
    val transformedTraining = fittedPipeline.transform(trainDataFrame)
    transformedTraining.cache()


    val kMeans = new KMeans()
      .setK(20)
      .setSeed(1L)

    val kmModel = kMeans.fit(transformedTraining)
    //kmModel.computeCost(transformedTraining)


    val transformedTest = fittedPipeline.transform(testDataFrame)
    //kmModel.computeCost(transformedTest)


    // Shows the result.
    println("Cluster Centers: ")
    kmModel.clusterCenters.foreach(println)


    spark.stop()
  }

}
