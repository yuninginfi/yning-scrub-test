package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.coders.common.AdLogRecord;
import com.linkedin.camus.etl.kafka.coders.common.ETLRecord;

public class KafkaTabSeperatedStringDecoder extends MessageDecoder<byte[], ETLRecord>{

	@Override
	public CamusWrapper<ETLRecord> decode(byte[] message) {
		String kafkaMessage = new String(message);
		String fields[] = kafkaMessage.split("\t");
		AdLogRecord record = new AdLogRecord(fields);
		return new CamusWrapper<ETLRecord>(record);
	}

}
