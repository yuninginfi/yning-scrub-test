package com.linkedin.camus.etl;

public class AdLogOutput {
	String timestamp;
	String partition_strategy;
	int bucketId;
	String tableName;
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getPartition_strategy() {
		return partition_strategy;
	}
	public void setPartition_strategy(String partition_strategy) {
		this.partition_strategy = partition_strategy;
	}
	public int getBucketId() {
		return bucketId;
	}
	public void setBucketId(int bucketId) {
		this.bucketId = bucketId;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
