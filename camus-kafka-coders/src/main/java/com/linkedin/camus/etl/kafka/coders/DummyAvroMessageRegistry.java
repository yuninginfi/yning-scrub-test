package com.linkedin.camus.etl.kafka.coders;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;

public class DummyAvroMessageRegistry extends MemorySchemaRegistry<Schema>{
    Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"DummyLog\",\"namespace\":\"com.linkedin.camus.etl.kafka.coders\",\"doc\":\"Logs for not so important stuff.\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"logTime\",\"type\":\"long\"}]}");
    Schema SCHEMA2$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"DummyLog\",\"namespace\":\"com.linkedin.camus.etl.kafka.coders\",\"doc\":\"Logs for not so important stuff.\",\"fields\":[{\"name\":\"eventType\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"bucketId\",\"type\":\"string\"},{\"name\":\"gogogo\",\"type\":\"string\"}]}"); 
    public DummyAvroMessageRegistry() {
        super();
        super.register("DummyLog", SCHEMA$);
        super.register("partition_test", SCHEMA2$);
    }
}
