package com.linkedin.camus.etl.kafka.common;

import java.util.List;

import org.apache.hadoop.io.MapWritable;

import com.linkedin.camus.etl.AdLogOutput;

public class EtlOutputKey extends EtlKey{
	List<AdLogOutput> outputs;

	@Override
	public List<AdLogOutput> getOutputs()
    {
		return outputs;
    };

	public void setOutputs(List<AdLogOutput> outputs) {
		this.outputs = outputs;
	}

	public EtlOutputKey(EtlKey other) {
		this.partition = other.partition;
		this.beginOffset = other.beginOffset;
		this.offset = other.offset;
		this.checksum = other.checksum;
		this.topic = other.topic;
		this.time = other.time;
		this.server = other.server;
		this.service = other.service;
		this.leaderId = other.leaderId;
        this.partitionMap = new MapWritable(other.partitionMap);
	}
	
   
}
