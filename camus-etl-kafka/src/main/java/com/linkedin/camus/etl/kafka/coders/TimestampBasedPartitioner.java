package com.linkedin.camus.etl.kafka.coders;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class TimestampBasedPartitioner extends DefaultPartitioner{
	
	private static Logger log = Logger.getLogger(TimestampBasedPartitioner.class);
	
	@Override
	public String encodePartition(JobContext context, IEtlKey key) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        return ""+DateUtils.getPartition(outfilePartitionMs, Long.valueOf(key.getOutputPartitionColumn()), outputDateFormatter.getZone());
    }

}
