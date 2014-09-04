package com.linkedin.camus.etl;

import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public interface IEtlKey {
    String getServer();

    String getService();

    long getTime();

    String getTopic();

    //String getNodeId();

    int getPartition();

    long getBeginOffset();

    long getOffset();

    long getChecksum();
    
    List<String> getOutputTopics();
    
    String getOutputPartitionColumn();
    
    int getOutputBucketingId();

    MapWritable getPartitionMap();

    void put(Writable key, Writable value);
}
