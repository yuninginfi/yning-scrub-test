package com.linkedin.camus.etl.kafka;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.producer.SyncProducerConfig;

public class KafkaMessageGenerator {
	public static void main(String args[])
	{
		System.out.println("start");
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "10.20.14.90:9092");
		props.put("zk.connect", "10.20.14.90:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for(long i = 0 ; i < 1000000 ; i ++)
		{
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("partition_test", "{\"eventType\":\"1\",\"timestamp\":\"2012-06-05\",\"bucketId\":\"4\",\"gogogo\":\"gogogo\"}");
			producer.send(data);
		}
		
		System.out.println("end");
	}
}
