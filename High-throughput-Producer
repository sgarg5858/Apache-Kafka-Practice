package com.github.sanjay.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	String consumerKey="***********************";
	String consumerSecret="*******************";
	String token="************************";
	String secret="*****************************";
	
	Logger logger=LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	//constructor
	public TwitterProducer() {}
	
	//main method
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run()
	{
		logger.info("setup");
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		//create Twitter Client
		Client client=createTwitterClient(msgQueue);
		client.connect();
		
		//Create Kafka Producer
		KafkaProducer<String, String>producer=createKafkaProducer();
		
		//Add Shutdown Hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Stopping Application");
			logger.info("Shutting Down client from twitter");
			client.stop();
			logger.info("closing producer");
			producer.close();
			logger.info("done!");
		}) );
		
		
		//Loop to send data to kafka
		String msg=null;
		while (!client.isDone()) {
			   try 
			   {
				   msg = msgQueue.poll(5l, TimeUnit.SECONDS);
			   }
			   catch (InterruptedException e) 
			   {
					e.printStackTrace();
					client.stop();
			   }
			   if(msg!=null)
			   {
				   logger.info(msg);
				   producer.send(new ProducerRecord<String, String>("twitter_data",null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception!=null)
						{
							logger.error("Something Bad Happened",exception);
						}
					}
				});
			   }
			}
		logger.info("End Of Application");
	}
	
	public Client createTwitterClient(BlockingQueue<String>msgQueue)
	{
		
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("malang","usa","india","bitcoin","soccer","cricket");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));                       // optional: use this if you want to process client events

				Client hosebirdClient = builder.build();
				return hosebirdClient;
	}
	public KafkaProducer<String, String> createKafkaProducer()
	{
String bootstrapServers="127.0.0.1:9092";
		
		//create Producer properties
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create Safe Producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		
		//high throughput settings
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		//create the producer
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
		return producer;
	}

}
