package com.learning.sparkstreaming.chapter2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

/****************************************************************************
 * This example is an example for Streaming Analytics in Spark.
 * It reads a real time orders stream from kafka, performs periodic summaries
 * and writes the output the a JDBC sink.
 ****************************************************************************/
public class StreamingAnalytics {

    public static void main(String[] args) {

        System.out.println("******** Initiating Streaming Analytics *************");

        //Needed for windows only
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Initiate MariaDB DB Tracker
        MariaDBManager dbManager = new MariaDBManager();
        dbManager.setUp();
        Thread dbThread = new Thread(dbManager);
        dbThread.start();

        //Initiate the Kafka Orders Generator
        KafkaOrdersDataGenerator ordersGenerator = new KafkaOrdersDataGenerator();
        Thread genThread = new Thread(ordersGenerator);
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
                    .appName("StreamingAnalyticsExample")
                    .getOrCreate();

            StructType schema = new StructType()
                    .add("orderId", IntegerType)
                    .add("product", StringType)
                    .add("quantity", IntegerType)
                    .add("price", DoubleType );

            System.out.println("Reading from Kafka..");
            Dataset<Row> rawOrdersDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "streaming.orders.input")
                    //.option("startingOffsets","earliest")
                    .load();

            //Convert JSON message in Kafka to a DataStream with columns
            Dataset<Row> ordersDf = rawOrdersDf
                    .selectExpr("CAST(value AS STRING)")
                    .select(functions.from_json(
                            functions.col("value"),schema).as("order"))
                    .select("order.orderId",
                            "order.product",
                            "order.quantity",
                            "order.price");


            //Write the output of each processed record to console
            ordersDf.writeStream().foreach(
                    new ForeachWriter<Row>() {

                        @Override public boolean open(long partitionId, long version) {
                            // Open connection
                            return true;
                        }
                        @Override public void process(Row record) {
                            // Write string to connection
                            System.out.println("Retrieved Order "
                                    + (new Timestamp(System.currentTimeMillis()))
                                    + " : " + record.toString() );
                        }
                        @Override public void close(Throwable errorOrNull) {
                            // Close the connection
                        }
                    }
            ).start();

            //Using Processing Time windows
            //Create windows of 5 seconds and compute total Order value by Product
            Dataset<Row> windowedSummary = ordersDf
                    .selectExpr("product",
                            "quantity * price as value")
                    .withColumn("timestamp", functions.current_timestamp())
                    .withWatermark("timestamp","5 seconds")
                    .groupBy(functions.window(
                            functions.col("timestamp"),
                            "5 seconds"),
                            functions.col("product"))
                    .agg(functions.sum(functions.col("value"))) ;

            //Write summary rows to MariaDB
            windowedSummary.writeStream()
                    .foreach(new MariaDBWriter())
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
