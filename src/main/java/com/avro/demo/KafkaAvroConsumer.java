package com.avro.demo;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import com.example.Customer;



public class KafkaAvroConsumer {

	public static void main(String[] args) {
		
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers","localhost:9092");
		properties.setProperty("group.id","avro-consumer");
		properties.setProperty("enable.auto.commit","false");
		properties.setProperty("auto.offset.reset","earliest");
		properties.setProperty("key.deserializer",StringDeserializer.class.getName());
		properties.setProperty("value.deserializer",KafkaAvroDeserializer.class.getName());
		properties.setProperty("schema.registry.url","http://localhost:8081");
		properties.setProperty("specific.avro.reader","true");

		KafkaConsumer<String,Customer> kafkaConsumer = new KafkaConsumer<String,Customer>(properties);
		String topic = "test-topic";		
		kafkaConsumer.subscribe(Collections.singleton(topic));		
		System.out.println("Waiting for data ...");
		
		while(true) {
			ConsumerRecords<String, Customer> consumerRecords = kafkaConsumer.poll(500);
			
			for (ConsumerRecord<String, Customer> consumerRecord : consumerRecords) {
				Customer customer = consumerRecord.value();
				System.out.println(customer);
			}
			kafkaConsumer.commitSync();
		}
		
	}

}
