package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.rocketfuel.grid.common.AdLogRecord;

public class AdLogRecordDecoder extends MessageDecoder<byte[], AdLogRecord>{
	private static AdLogRecord record = new AdLogRecord();
	private static CamusWrapper<AdLogRecord> wrapper = new CamusWrapper<AdLogRecord>(null);
	private static org.apache.log4j.Logger log = Logger.getLogger(AdLogRecordDecoder.class);

	@Override
	public CamusWrapper<AdLogRecord> decode(byte[] message) {
		
		try {
			record.clear();
		    record.set(new String(message));
		    wrapper.setRecord(record);
			return wrapper;
		} catch (IOException e) {
			log.error("Error reading message:" + message.toString());
			throw new RuntimeException(e);
		}
	}
}
