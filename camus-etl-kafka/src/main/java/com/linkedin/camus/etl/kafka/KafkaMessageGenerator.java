package com.linkedin.camus.etl.kafka;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import kafka.api.PartitionFetchInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.producer.SyncProducerConfig;

public class KafkaMessageGenerator {
	public static void main(String args[])
	{
		System.out.println("start");
		
		SimpleConsumer simpleConsumer = new SimpleConsumer("lsv-50.rfiserve.net", 9092,
				1000,
				100000,
				"yning_test");
		
		TopicAndPartition topicAndPartition = new TopicAndPartition(
				"AdBeaconServer", 50);
		PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(
				1761361, 100000);
		
		HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
		fetchInfo.put(topicAndPartition, partitionFetchInfo);
		
		FetchRequest fetchRequest = new FetchRequest(
				100000,
				"yning_test",
				100000,
				1, fetchInfo);
		FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
		
		ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(
				"AdBeaconServer", 50);
		System.out.println(messageBuffer);
	
		Iterator<MessageAndOffset> messageIter = messageBuffer.iterator();
		
		int i = 0;
		while(messageIter.hasNext())
		{
			messageIter.next();
			System.out.println(i++);
		}
		if (!messageIter.hasNext()) {
			System.out
					.println("No more data left to process. Returning false");
			messageIter = null;
		}
		
		

		
		/*
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
		}*/
		
		System.out.println("end");
	}
}
