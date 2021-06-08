package com.spark_streaming

import com.spark_streaming.Utilities.setupLogging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Example of using streaming linear regression with stochastic gradient descent.
 * Listens to port 9999 for data on page speed vs. amount spent, and creates
 * a linear model to predict amount spent by page speed over time.
 */
object StreamingRegression {

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "StreamingRegression", Seconds(1))

    setupLogging()

    val trainingLines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val testingLines = ssc.socketTextStream("127.0.0.1", 7777, StorageLevel.MEMORY_AND_DISK_SER)

    // Convert input data to LabeledPoints for MLLib
    val trainingData = trainingLines.map(LabeledPoint.parse).cache()
    val testData = testingLines.map(LabeledPoint.parse)

    // Just so we see something happen when training data is received
    trainingData.print()

    // Now we will build up our linear regression model as new training data is received.
    val numFeatures = 1
    val model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures))
    model.algorithm.setIntercept(true) // Needed to allow the model to have a non-zero Y intercept

    model.trainOn(trainingData)

    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    // Kick it off
    ssc.checkpoint("/Users/yuyatinnefeld/desktop/projects/spark/scala/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
