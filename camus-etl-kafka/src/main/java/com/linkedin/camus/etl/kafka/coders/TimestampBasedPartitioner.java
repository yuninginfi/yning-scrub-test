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
		log.info("encoding:" + key.getOutputPartitionColumn());
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        Date timestamp = new Date();
		try {
			timestamp = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(key.getOutputPartitionColumn());
			log.info("Found timestamp:" + timestamp.toString());
		} catch (ParseException e) {
			e.printStackTrace();
		}
        return ""+DateUtils.getPartition(outfilePartitionMs, timestamp.getTime(), outputDateFormatter.getZone());
    }

}
