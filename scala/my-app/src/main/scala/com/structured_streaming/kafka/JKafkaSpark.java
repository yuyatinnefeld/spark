package com.structured_streaming.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.OutputMode;

public class JKafkaSpark {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .config("spark.driver.host","127.0.0.1")
                .config("spark.sql.shuffle.partitions",2)
                .config("spark.default.parallelism",2)
                .appName("StreamingAnalyticsExample")
                .getOrCreate();

        System.out.println("Reading from Kafka..");

        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "spark_kafka")
                .option("failOnDataLoss","false")
                .load();


        kafkaStream.isStreaming();



    }
}
