package com.kafka.ConsumerDemo;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		
		Logger logger=LoggerFactory.getLogger(ConsumerDemo.class.getName());
		String bootstrapServers="localhost:9092";
		String groupId="my_fifth_application";
		String topic="first_topic";
		//Set Property
		Properties properties=new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		//Create Consumer
		KafkaConsumer<String, String>consumer=new KafkaConsumer<String, String>(properties);
		
		
		// Subscribe
		consumer.subscribe(Collections.singleton(topic));
		
		//poll
		while(true)
		{
			ConsumerRecords<String, String>records=   
													consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record:records)
			{
				logger.info("key: "+record.key()+" value: "+record.value()+" partition "+record.partition()
				+" Offset: "+record.offset());
			}
		}
	}
}
