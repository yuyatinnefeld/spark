package com.spark_mllib

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql._

object DecisionTreeHousePrice {

  case class HouseSchema(No:Int, TransactionDate:Double, HouseAge:Double, DistanceToMRT:Double,
                         NumberConvenienceStores:Int, Latitude:Double, Longitude:Double, PriceOfUnitArea:Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("DecisionTree")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val rawDS = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/ml/houseprice.csv")
      .as[HouseSchema]

    println("### Data Load ###")
    rawDS.show()

    //Spark MlLib Feature VectorAssembler
    val assembler = new VectorAssembler()
      .setInputCols(Array("HouseAge", "DistanceToMRT", "NumberConvenienceStores"))
      .setOutputCol("features")

    val transformedDF = assembler.transform(rawDS)
      .select("PriceOfUnitArea", "features")

    println("### Data Transform ###")
    transformedDF.show()

    val trainTest = transformedDF.randomSplit(Array(0.6, 0.4))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    val DTR = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")


    println("### Model Training ###")
    val model = DTR.fit(trainingDF)


    println("### Prediction ###")
    val fullPredictions = model.transform(testDF).cache()
    val predictionAndLabel = fullPredictions.select("prediction", "PriceOfUnitArea").collect()

    predictionAndLabel.foreach(println)

    spark.stop()

  }

}
