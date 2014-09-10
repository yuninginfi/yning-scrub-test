package com.linkedin.camus.etl.kafka;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaDataLoader {
	public static void main(String args[])
	{
		System.out.println("start");
		/*
		SimpleConsumer simpleConsumer = new SimpleConsumer("inw-100.rfiserve.net", 9092,
				1000,
				100000,
				"yning_test");
		
		TopicAndPartition topicAndPartition = new TopicAndPartition(
				"AdBeaconServer", 0);
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
		
		

		
		*/
		
		
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "10.40.42.40:9092");
		props.put("zk.connect", "10.40.42.40:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader("/tmp/AdBeaconServer1M_2"));
			String line = null;
			int num = 0;
			while ((line = reader.readLine()) != null) {
				if(++num % 1000 == 0)
					System.out.println(num);
				KeyedMessage<String, String> data = new KeyedMessage<String, String>("adlog_2", line);
				producer.send(data);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		System.out.println("end");
	}
}
