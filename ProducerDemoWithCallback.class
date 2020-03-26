package com.kafka.ProducerDemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(String[] args) {
		
		final Logger logger=LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		String bootstrapServers="localhost:9092";
		
		//Create Properties
		Properties properties=new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String>producer=new KafkaProducer<String, String>(properties);
		for(int i=0;i<10;i++)
		{
			//Create The Producer
		
			
			//Create Producer Record
			ProducerRecord<String, String>record=new ProducerRecord<String, String>("first_topic","Hello Mujj"+Integer.toString(i+20));
			//Send data
			//asynchronous
			producer.send(record,new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// executes every time record is sent or exception is thrown
					if(exception==null)
					{
						logger.info("Data Successfully sent to  "+"Topic: "+metadata.topic()  +" Partition: "+metadata.partition()+"\n");
					}
					else
					{
						logger.error(exception.getMessage());
					}
				}
			});
			
		
		
		}
		//wait operation
		producer.flush();
		producer.close();
	}
}
