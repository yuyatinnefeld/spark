package com.learning.sparkstreaming.chapter5;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.net.URI;
import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This example is an example for Streaming Predictions in Spark.
 * It reads a real time movie reviews stream from kafka
 * and predicts sentiment of the review
 ****************************************************************************/
public class StreamingPredictions {

    public static void main(String[] args) {

        System.out.println("******** Initiating Streaming Predictions *************");

        //For windows only
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Initiate the Kafka Reviews Generator
        KafkaReviewsDataGenerator reviewsGenerator = new KafkaReviewsDataGenerator();
        Thread genThread = new Thread(reviewsGenerator);
        genThread.start();

        System.out.println("******** Starting Streaming  *************");

        try {
            //Create the Spark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.sql.shuffle.partitions",2)
                    .config("spark.default.parallelism",2)
                    .appName("StreamingPredictionsExample")
                    .getOrCreate();

            System.out.println("Reading from Kafka..");
            Dataset<Row> rawReviewsDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "streaming.sentiment.input")
                    //.option("startingOffsets","earliest")
                    .load();

            //Convert  message in Kafka to a DataStream with columns
            Dataset<Row> reviewsDf = rawReviewsDf
                    .selectExpr("CAST(key AS STRING) as id",
                                    "CAST(value AS STRING) as review");

            //Define a User Defined Function for getting sentiment
            UDF1 getSentiment = new UDF1<String,String>() {

                @Override
                public String call(String review) throws Exception {
                    return SentimentPredictor
                            .getSentiment(review);
                }
            };

            //Register the UDF function.
            spark.sqlContext().udf()
                    .register("getSentiment",
                            getSentiment, DataTypes.StringType);

            //Use UDF to get sentiment for each review
            Dataset<Row> sentimentDf = reviewsDf
                    .selectExpr("id","review", "getSentiment(review) as sentiment");

            //Write the output of each processed record to console
            sentimentDf.writeStream().foreach(
                    new ForeachWriter<Row>() {

                        @Override public boolean open(long partitionId, long version) {
                            // Open connection
                            return true;
                        }
                        @Override public void process(Row record) {
                            // Write string to connection
                            System.out.println("Output - "
                                    + "Sentiment= " + record.getString(2)
                                    + " : for " + record.getString(1));
                        }
                        @Override public void close(Throwable errorOrNull) {
                            // Close the connection
                        }
                    }
            ).start();

            //Publish the sentiment
            sentimentDf
                    .selectExpr("id as key", "sentiment as value")
                    .writeStream()
                    .format("kafka")
                    .option("checkpointLocation", "/tmp/review-sentiment")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "streaming.sentiment.output")
                    .start();

            //Keep the process running
            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }



    }
}
