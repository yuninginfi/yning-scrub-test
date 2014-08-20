package com.linkedin.camus.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;

public class LatestSchemaKafkaAvroMessageDecoder extends KafkaAvroMessageDecoder
{
    private static Logger log = Logger.getLogger(LatestSchemaKafkaAvroMessageDecoder.class);
	@Override
	public CamusWrapper<Record> decode(byte[] payload)
	{
		try
		{
		    
			GenericDatumReader<Record> reader = new GenericDatumReader<Record>();
			log.info("Topic is :" + super.topicName);
			Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();
			log.info("Schema is:" + schema.toString());
			reader.setSchema(schema);
			Record a;
			log.info("whole message is:" + new String(payload));
			log.info("MagicOffset is:" + Message.MagicOffset());
			log.info("Record is:" + new String(
                                    payload, 
                                    //Message.payloadOffset(message.magic()),
                                    Message.MagicOffset(),
                                    payload.length - Message.MagicOffset()
                            ));
			return new CamusWrapper<Record>(reader.read(
                    null, 
                    decoderFactory.jsonDecoder(
                            schema, 
                            new String(
                                    payload
                            )
                    )
            ));
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}