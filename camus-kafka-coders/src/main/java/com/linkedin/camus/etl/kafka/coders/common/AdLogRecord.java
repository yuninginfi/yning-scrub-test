package com.linkedin.camus.etl.kafka.coders.common;

public class AdLogRecord implements ETLRecord {
	private String eventType;
	private String timestamp;
	private String bucketId;
	private String extraColumn;
	
	public AdLogRecord(String row) {
		String[] fields = row.split("\t");
		eventType = "topic_" + String.valueOf(Long.valueOf(fields[5]) % 10);
		timestamp = fields[5];
		bucketId = String.valueOf(Math.abs(Long.valueOf(fields[3])) % 64);
		extraColumn = row;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getBucketId() {
		return bucketId;
	}

	public void setBucketId(String bucketId) {
		this.bucketId = bucketId;
	}

	public String getExtraColumn() {
		return extraColumn;
	}

	public void setExtraColumn(String extraColumn) {
		this.extraColumn = extraColumn;
	}

	@Override
	public String toString()
	{
		return eventType + "\t" + timestamp + "\t" + bucketId + "\t" + extraColumn;
	}
	
	@Override
	public void setFields(String[] fields) {
		eventType = fields[0];
		timestamp = fields[1];
		bucketId = fields[2];
		extraColumn = fields[3];
	}

}
