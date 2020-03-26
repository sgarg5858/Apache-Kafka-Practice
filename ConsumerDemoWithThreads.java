package com.kafka.ConsumerDemo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {

	
	public ConsumerDemoWithThreads()
	{
	}
		void run()
		{
		final Logger logger=LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
		String bootstrapServers="localhost:9092";
		String groupId="my_sixth_application";
		String topic="first_topic";
		
		CountDownLatch latch=new CountDownLatch(1);
		
		final Runnable myConsumerThread=new ConsumerThread(latch, bootstrapServers, groupId, topic);	
		
		logger.info("Creating CONSUMER THREAD");
		Thread myThread=new Thread(myConsumerThread);
		myThread.start();
		
		//Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Caught shut down hook");
			((ConsumerThread)myConsumerThread).shutdown();
		}
		));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("Application got Interrupted");
		}finally {
			logger.info("Application shutdown");
		}
		}

	public static void main(String[] args) {
		
		
		new ConsumerDemoWithThreads().run();
		
	}
	
	public class ConsumerThread implements Runnable{

		private CountDownLatch latch;
		private KafkaConsumer<String, String>consumer;
		private Logger logger=LoggerFactory.getLogger(ConsumerThread.class.getName());
		public ConsumerThread(CountDownLatch latch,String bootstrapServers,String groupId,String topic)
		{
			
			this.latch=latch;
			
			Properties properties=new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			this.consumer=new KafkaConsumer<String, String>(properties);
			
			consumer.subscribe(Collections.singleton(topic));
		}
		public void run() {
			try {
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
				
			}catch(WakeupException e)
			{
				logger.info("Recieved Consumer ShutDown Call");
			}finally {
				consumer.close();
				//tell our main method we are done with consumer
				latch.countDown();
			}
		}
		public void shutdown()
		{
			//this .wakeup() method is a special method to interrupt consumer.poll() method
			//it will throw wakeup Exception
			consumer.wakeup();
		}
	}
}

