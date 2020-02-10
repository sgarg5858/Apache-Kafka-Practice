# Apache-Kafka-Practice

How to create high-throughput-producer while maintaining ordering of messages int topics and ensuring no data loss?

We know if the producer is able to send messages fast the whole system benefits.

Basic Properties:
//create Producer properties
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

1.No data loss
(i) For ensuring no data loss while sending data to kafka(topics/partitions) we must use (ack="all) this will need acknowledgement from
leader and replicas.

(ii) And we have to set min.insync.replicas (how many brokers are required to send an acknowledgment incuding leader we have to choose this wisely.)

(iii)This will add a bit of latency but ensure no data loss.


2.High throughput
(i) Instead of sending messages alone we shouls send them in batches as we know batches are send per partition basis when sending messages with key so messages which are going to same parition can be batch together other wise not.This can be improved a bit by Sticky Partitoner.
linger.ms is property that tells for how much time producer should wait before send the batch if batch size is full before then it will send right away.
This can be set by 
properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024))
properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
at the producer level.

(ii).we can also apply compression to batches to compress the size to send them fast over the network.
//	properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


3.Order Maintain: 
(i) Suppose if data is not recieved by broker or acknowledment is not recieved by producer it will retry then the order can be changed as there can be multiple requests in parallel.
properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

(ii) How often will producer retry?
retry.backoff.ms=100(by default)

(iii) will producer try forever?
No.

delivery.timeout.ms=120000(2 mins) by default.

But how to maintain order as multiple requests can be in parallel.

i).Set ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION=1;

Doing this can impact your throughput but will maintain order.

ii). Use idempotent producer

It maintains order by using producer and sequential id for every parition tracked by producer and on the broker side per-parition basis.
If last id=newid-1 then partition won't add the record.

1.acks=all;
2. ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION=5; by default;

this can be done by enable.idempotence=true (producer level).


4. max.block.ms=60000 by default.

if producer is producing at very high speed then the speed of consumer this can lead to overflow of buffer memory of producer which can give error.


