package com.learning.sparkstreaming.chapter4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This example is an example for Streaming leaderboards in Spark.
 * It reads a real time player scores stream from kafka
 * and updates a sortedset in Redis
 ****************************************************************************/
public class StreamingLeaderboards {

    public static void main(String[] args) {

        System.out.println("******** Initiating Streaming Leaderboards *************");

        //Needed for windows only
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Initiate  RedisManager
        RedisManager redisManager = new RedisManager();
        redisManager.setUp();
        Thread redisThread = new Thread(redisManager);
        redisThread.start();

        //Initiate the Kafka Orders Generator
        KafkaGamingDataGenerator gamingGenerator = new KafkaGamingDataGenerator();
        Thread genThread = new Thread(gamingGenerator);
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
                    .appName("StreamingLeaderboardsExample")
                    .getOrCreate();

            System.out.println("Reading from Kafka..");

            //Consume the leaderboard topic
            Dataset<Row> rawScoresDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "streaming.leaderboards.input")
                    //.option("startingOffsets","earliest")
                    .load();

            //Convert  message in Kafka to a DataStream with key-value
            Dataset<Row> scoresDf = rawScoresDf
                    .selectExpr("CAST(key AS STRING) as player",
                                    "CAST(value AS STRING) as score");

            //Write the output of each processed record to Redis
            scoresDf
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
