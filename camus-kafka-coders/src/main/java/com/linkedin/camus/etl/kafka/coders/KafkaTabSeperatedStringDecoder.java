package com.linkedin.camus.etl.kafka.coders;

import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.coders.common.AdLogRecord;
import com.linkedin.camus.etl.kafka.coders.common.ETLRecord;

public class KafkaTabSeperatedStringDecoder extends MessageDecoder<byte[], ETLRecord>{

	private static Logger log = Logger.getLogger(KafkaTabSeperatedStringDecoder.class);
	@Override
	public CamusWrapper<ETLRecord> decode(byte[] message) {
		String kafkaMessage = new String(message);
		String fields[] = kafkaMessage.split("\t");
		AdLogRecord record = new AdLogRecord(kafkaMessage);
		return new CamusWrapper<ETLRecord>(record);
	}

}
