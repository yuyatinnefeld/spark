package com.learning.sparkstreaming.chapter3;

import com.learning.sparkstreaming.chapter2.KafkaOrdersDataGenerator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;

import static org.apache.spark.sql.types.DataTypes.*;

public class StreamingAlertsAndThresholds {

    public static void main(String[] args) {

        System.out.println("******** Initiating Streaming Alerts and Thresholds *************");

        //needed for windows only.
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Initiate the Kafka alerts Generator
        KafkaAlertsDataGenerator alertsGenerator = new KafkaAlertsDataGenerator();
        Thread genThread = new Thread(alertsGenerator);
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
                    .appName("StreamingAlertsAndThresoldsExample")
                    .getOrCreate();

             System.out.println("Reading from Kafka..");
            Dataset<Row> rawAlertsDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "streaming.alerts.input")
                    .option("failOnDataLoss","false")
                    //.option("startingOffsets","earliest")
                    .load();


            Dataset<Row> alertsDf = rawAlertsDf
                    .selectExpr("translate(value,'\"','') as value")
                    .selectExpr("cast(split(value,',')[0] as TIMESTAMP) as timestamp",
                            "split(value,',')[1] as level",
                            "split(value,',')[2] as code",
                            "split(value,',')[3] as message" );

            alertsDf
                    .writeStream().foreach(
                    new ForeachWriter<Row>() {

                        @Override public boolean open(long partitionId, long version) {
                            return true;
                        }
                        @Override public void process(Row record) {
                            System.out.println("Retrieved Alert "
                                    + " : " + record.toString() );
                        }
                        @Override public void close(Throwable errorOrNull) {
                            // Close the connection
                        }
                    }
            ).start();


            //Filter all Critical Alerts
            Dataset<Row> criticalDf = alertsDf
                    .filter( "level == 'CRITICAL'");

            //Write Critical alerts to an outgoing Kafka topic.
            criticalDf
                    .selectExpr("format_string(\"%s,%s,%s\", timestamp,code,message) as value")
                    .writeStream()
                    .format("kafka")
                    .option("checkpointLocation", "/tmp/cp-critical")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "streaming.alerts.critical")
                    .start();


            //Find codes that occur more than 2 times in a 5 sec interval
            Dataset<Row> codeSummary = alertsDf
                    .withWatermark("timestamp","10 seconds")
                    .groupBy(functions.window(
                            functions.col("timestamp"),
                            "10 seconds"),
                            functions.col("code"))
                    .count()
                    .filter("count > 2");


            codeSummary
                    .selectExpr("format_string(\"%s,%s,%d\", window.start,code,count) as value")
                    .writeStream()
                    .format("kafka")
                    .option("checkpointLocation", "/tmp/cp-summary")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "streaming.alerts.highvolume")
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
