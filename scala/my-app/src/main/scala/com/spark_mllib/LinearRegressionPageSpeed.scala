package com.spark_mllib

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

object LinearRegressionPageSpeed {

  case class Regression(label:Double, features_row:Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LR")
      .master("local[*]")
      .getOrCreate()

    val regressionSchema = new StructType()
      .add("label", DoubleType, nullable = true)
      .add("features_row", DoubleType, nullable = true)

    import spark.implicits._
    val rawDS = spark.read
      .option("sep", ",")
      .schema(regressionSchema)
      .csv("data/ml/regression.txt")
      .as[Regression]

    println("### Data Load ###")
    rawDS.show()

    //Spark MlLib Feature VectorAssembler
    val assembler = new VectorAssembler()
      .setInputCols(Array("features_row"))
      .setOutputCol("features")

    val transformedDS = assembler.transform(rawDS)
      .select("label", "features")

    println("### Data Transform ###")
    transformedDS.show()

    val trainTest = transformedDS.randomSplit(Array(0.6, 0.4))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    //Spark MlLib Feature LinearRegression
    val LR = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(108)
      .setTol(1E-6)

    println("### Model Training ###")
    val model = LR.fit(trainingDF)

    println("### Prediction ###")
    val fullPredictions = model.transform(testDF).cache()
    val predictionAndLabel = fullPredictions.select("prediction", "label").collect()
    predictionAndLabel.foreach(println)

    spark.stop()

  }

}
