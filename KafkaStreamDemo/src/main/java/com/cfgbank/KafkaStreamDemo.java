package com.cfgbank;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class KafkaStreamDemo {

	public static void main(String[] args) {
		System.out.println("kafka stream is running");
		String inputTopic = "GuruTopic5";
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, 
				"wordcount-live-test");
		String bootstrapServers = "localhost:9092";
		streamsConfiguration.put(
				StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
				bootstrapServers);
		streamsConfiguration.put(
				StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, 
				Serdes.String().getClass().getName());
		streamsConfiguration.put(
				StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, 
				Serdes.String().getClass().getName());
		streamsConfiguration.put(
				StreamsConfig.STATE_DIR_CONFIG, 
				"c:\\kafka-stream-test-dir");
		
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> textLines = builder.stream(inputTopic);
		textLines.foreach((key,val)->{System.out.println(val.toUpperCase());});
		
		Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
		KTable<String, Long> wordCounts = textLines
				  .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
				  .groupBy((key, word) -> word)
				  .count();
		wordCounts.foreach((w, c) -> System.out.println("word: " + w + " -> " + c));
		KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
		streams.start();
//		while (true)
//		{
//			
//		}
//		try {
//			Thread.sleep(15000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		

            while (true) {
              
            }
       

	}

}
