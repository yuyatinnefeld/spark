package com.learning.sparkstreaming.chapter6;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;

import static org.apache.spark.sql.types.DataTypes.*;

/****************************************************************************
 * This is an example for Website Views Analytics in Spark.
 * It reads a real time views from Kafka, computes 5 second user summaries
 * and keeps track of a leaderboard for topics with maximum views
 ****************************************************************************/
public class WebsiteViewsAnalytics {

    public static void main(String[] args) {

        System.out.println("******** Initiating Website Views Analytics *************");

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Initiate  RedisManager
        RedisManager redisManager = new RedisManager();
        redisManager.setUp();
        Thread redisThread = new Thread(redisManager);
        redisThread.start();

        //Initiate the Kafka Orders Generator
        KafkaViewsDataGenerator viewsGenerator = new KafkaViewsDataGenerator();
        Thread genThread = new Thread(viewsGenerator);
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
                    .appName("WebsiteAnalyticsExample")
                    .getOrCreate();

            System.out.println("Reading from Kafka..");

            Dataset<Row> rawViewsDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "streaming.views.input")
                    //.option("startingOffsets","earliest")
                    .load();

            //Convert  message in Kafka to a DataStream with columns
            Dataset<Row> viewsDf = rawViewsDf
                    .selectExpr("translate(value,'\"','') as value")
                    .selectExpr("cast(split(value,',')[0] as TIMESTAMP) as timestamp",
                            "split(value,',')[1] as user",
                            "split(value,',')[2] as topic",
                            "CAST(split(value,',')[3] as INTEGER) as minutes" );


            //Write the output of each processed record to console
            viewsDf.writeStream().foreach(
                    new ForeachWriter<Row>() {

                        @Override public boolean open(long partitionId, long version) {
                            // Open connection
                            return true;
                        }
                        @Override public void process(Row record) {
                            // Write string to connection
                            System.out.println("Retrieved View Record "
                                + (new Timestamp(System.currentTimeMillis()))
                                    + " : " + record.toString() );
                        }
                        @Override public void close(Throwable errorOrNull) {
                            // Close the connection
                        }
                    }
            ).start();

            //Using Event Time windows
            //Create windows of 5 seconds and compute total minutes by user
            Dataset<Row> windowedSummary = viewsDf
                    .withWatermark("timestamp","5 seconds")
                    .groupBy(functions.window(
                                functions.col("timestamp"),
                             "5 seconds"),
                            functions.col("user"))
                    .agg(functions.sum(functions.col("minutes"))) ;

            //Write summary rows to console
            windowedSummary.writeStream().foreach(
            new ForeachWriter<Row>() {

                @Override
                public boolean open(long partitionId, long version) {
                    return true;
                }

                @Override
                public void process(Row record) {
                    System.out.println("User View Summary : "
                            + " : " + record.toString());
                }

                @Override
                public void close(Throwable errorOrNull) {
                    // Close the connection
                }
            }
            ).start();

            //Compute the Leaderboard for Topics
            viewsDf
                    .selectExpr("topic", "1")
                    .writeStream()
                    .foreach(new RedisWriter())
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
