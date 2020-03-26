package com.kafka.ProducerDemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		
		String bootstrapServers="localhost:9092";
		
		//Create Properties
		Properties properties=new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create The Producer
		KafkaProducer<String, String>producer=new KafkaProducer<String, String>(properties);
		
		//Create Producer Record
		ProducerRecord<String, String>record=new ProducerRecord<String, String>("first_topic","Hello Mujj");
		//Send data
		//asynchronous
		producer.send(record);
		
		//wait operation
		producer.flush();
		producer.close();
	}
}
