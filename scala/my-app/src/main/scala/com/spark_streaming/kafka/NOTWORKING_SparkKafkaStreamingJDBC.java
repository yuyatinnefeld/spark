
package com.spark_streaming.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class NOTWORKING_SparkKafkaStreamingJDBC {

	public static void main(String[] args) {
		
		//Setup log levels so there is no slew of info logs
		//Logger.getLogger("org").setLevel(Level.ERROR);
		
		//Start a spark instance and get a context
		SparkConf conf = new SparkConf()
						.setMaster("local[2]")
						.setAppName("Study Spark");
		
		//Setup a streaming context.
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(3));
		
		//Create a map of Kafka params
		Map<String, Object> kafkaParams = new HashMap<String,Object>();

		//List of Kafka brokers to listen to.
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "group_yu");

		//Do you want to start from the earliest record or the latest?
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);

		//List of topics to listen to.
		Collection<String> topics = Arrays.asList("topicYY", "test");

		//Create a Spark DStream with the kafka topics.
		final JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
		    streamingContext,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );

		stream.print();

		//Start streaming.
		streamingContext.start();
		
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//streamingContext.close();
		
		//Keep the program alive.
		while( true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}


}
