package com.linkedin.camus.etl.kafka.common;

import org.apache.hadoop.io.MapWritable;

public class EtlOutputKey extends EtlKey{
	String outputTopic;
	String outputPartitionColumn;
	int outputBucketingId;

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
	
    @Override
	public String getOutputTopic() {
		return outputTopic;
	}

	public void setOutputTopic(String outputTopic) {
		this.outputTopic = outputTopic;
	}
	
	@Override
	public String getOutputPartitionColumn() {
		return outputPartitionColumn;
	}
	public void setOutputPartitionColumn(String outputPartitionColumn) {
		this.outputPartitionColumn = outputPartitionColumn;
	}
	
	@Override
	public int getOutputBucketingId() {
		return outputBucketingId;
	}
	public void setOutputBucketingId(int outputBucketingId) {
		this.outputBucketingId = outputBucketingId;
	}

}
