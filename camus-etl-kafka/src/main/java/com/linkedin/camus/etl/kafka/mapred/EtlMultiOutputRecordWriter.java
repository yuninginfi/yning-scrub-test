package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.AdLogOutput;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.OutputsCollection;
import com.rocketfuel.grid.common.AdLogEventType;
import com.rocketfuel.grid.common.AdLogRecord;
import com.rocketfuel.grid.common.AdLogRecord.AdFields;
import com.rocketfuel.grid.common.AdLogRecord.Fields;
import com.rocketfuel.grid.common.Utils;

public class EtlMultiOutputRecordWriter extends RecordWriter<EtlKey, Object> {
	private TaskAttemptContext context;
	private Writer errorWriter = null;
	private String currentTopic = "";
	private long beginTimeStamp = 0;
	private long milisecondsInDay = 86400000l;
	private static Logger log = Logger
			.getLogger(EtlMultiOutputRecordWriter.class);
	private static OutputsCollection outputsCollection = new OutputsCollection();

	private Map<String, Integer> tableBucketsMap = new HashMap<String, Integer>() {
		{
			put("cookie_matching", 64);
			put("cookie", 64);
			put("actions", 64);
		}
	};
	
	private static HashMap<String, AdLogOutput> outputs;

	private HashMap<String, RecordWriter<IEtlKey, CamusWrapper>> dataWriters = new HashMap<String, RecordWriter<IEtlKey, CamusWrapper>>();

	private EtlMultiOutputCommitter committer;

	public EtlMultiOutputRecordWriter(TaskAttemptContext context,
			EtlMultiOutputCommitter committer) throws IOException,
			InterruptedException {
		outputsCollection.ini();
		this.context = context;
		this.committer = committer;
		errorWriter = SequenceFile.createWriter(
				FileSystem.get(context.getConfiguration()),
				context.getConfiguration(),
				new Path(committer.getWorkPath(), EtlMultiOutputFormat
						.getUniqueFile(context,
								EtlMultiOutputFormat.ERRORS_PREFIX, "")),
				EtlKey.class, ExceptionWritable.class);

		if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
			int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
			beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
		} else {
			beginTimeStamp = 0;
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		for (String w : dataWriters.keySet()) {
			dataWriters.get(w).close(context);
		}
		errorWriter.close();
	}

	@Override
	public void write(EtlKey key, Object val) throws IOException,
			InterruptedException {
		if (val instanceof CamusWrapper<?>) {
			if (key.getTime() < beginTimeStamp) {
				// ((Mapper.Context)context).getCounter("total",
				// "skip-old").increment(1);
				committer.addOffset(key);
			} else {
				if (!key.getTopic().equals(currentTopic)) {
					for (RecordWriter<IEtlKey, CamusWrapper> writer : dataWriters
							.values()) {
						writer.close(context);
					}
					dataWriters.clear();
					currentTopic = key.getTopic();
				}

				// New output

				CamusWrapper value = (CamusWrapper) val;

				AdLogRecord record = (AdLogRecord) value.getRecord();
				key.setOutputs(getOutputs(record));

				committer.addCounts(key);

				for (AdLogOutput output : key.getOutputs()) {
					String newWorkingFileName = EtlMultiOutputFormat
							.getWorkingFileName(context, key, output);
					if (!dataWriters.containsKey(newWorkingFileName)) {
						log.info("NewWorkingFileName does not exist. Creating recordWriter for "
								+ newWorkingFileName);
						context.getCounter("total", "open-file").increment(1);
						context.getCounter("total", output.getTableName())
								.increment(1);
						log.info("Current # of open files is:"
								+ context.getCounter("total", "open-file")
										.getValue());
						dataWriters.put(
								newWorkingFileName,
								getDataRecordWriter(context,
										newWorkingFileName, value));
					}
					dataWriters.get(newWorkingFileName).write(key, value);
				}

			}
		} else if (val instanceof ExceptionWritable) {
			committer.addOffset(key);
			System.err.println(key.toString());
			System.err.println(val.toString());
			errorWriter.append(key, (ExceptionWritable) val);
		}
	}

	private static List<AdLogOutput> outputTBLs = new ArrayList<AdLogOutput>();

	List<AdLogOutput> getOutputs(AdLogRecord log_record) {
		outputTBLs.clear();

		try {

			// *** extract commonly used columns
			String eventType = log_record.getColumn(Fields.EVENT);
			Utils.getDateStampFromUnixTimeStamp(
					log_record.getColumn(Fields.SERVER_TIMESTAMP), "yyyy/MM/dd");
			Integer.parseInt(log_record.getColumn(Fields.LOG_FORMAT_NUMBER));
			log_record.getColumn(Fields.USER_AGENT);
			String userId = log_record.getColumn(Fields.USER_ID);
			String ip = log_record.getColumn(Fields.IP);
			String url = log_record.getColumn(Fields.URL);
			log_record.getColumn(Fields.PLACEMENT_VERSION_ID);
			String requestId = log_record.getColumn(Fields.REQUEST_ID);
			boolean isEmptyCookie = log_record.getColumn(Fields.COOKIES)
					.isEmpty()
					&& log_record.getColumn(Fields.MODIFIED_COOKIES).isEmpty();

			String timestampStr = log_record.getColumn(Fields.SERVER_TIMESTAMP);
			long timestampLong = Long.valueOf(timestampStr);
			String timestampInDayStr = String.valueOf(timestampLong
					- timestampLong % milisecondsInDay);

			// Inner try is for
			// 1. isTesting Record which does a IP Lookup which can throw an
			// exception
			// 2. isBot which can also throw an exception
			// And we don't want to lose this record if the checks fail or spew
			// an exception
			try {
				if (isTestingRecord(userId, url, ip)) {
					AdLogOutput outputs = outputsCollection.getOutput("Bots");
					outputs.setTimestamp(timestampInDayStr);
					outputTBLs.add(outputs);
					return outputTBLs;
				}

			}// EO Inner Try
			catch (Exception e) {
				log.warn("Not filterting this record. Caught error in bot check "
						+ e.getMessage());
			}

			// *** core data collection logic

			// 1. collect cookie data
			if (!isEmptyCookie) {
				AdLogOutput outputs = outputsCollection.getOutput("Cookies");
				outputs.setBucketId(Math.abs((int) ((Long.valueOf(userId) % 64))));
				outputs.setTimestamp(timestampStr);
				outputTBLs.add(outputs);
			}

			// 2. collect clicks data
			if (AdLogEventType.AdClick.equals(AdLogEventType.get(eventType))) {
				AdLogOutput outputs = outputsCollection.getOutput("Clicks");
				outputs.setTimestamp(timestampInDayStr);
				outputTBLs.add(outputs);
			}

			// 3. collect rtb video data - { the event type could be video view
			// (ve), all the sub event types are logged}
			if (AdLogEventType.Video.equals(AdLogEventType.get(eventType))) {
				AdLogOutput outputs = outputsCollection.getOutput("Rtb_video");
				outputs.setTimestamp(timestampInDayStr);
				outputTBLs.add(outputs);
			}

			// 4. collect beacon and external user segment records.
			if (AdLogEventType.BeaconView.equals(AdLogEventType.get(eventType))
					|| AdLogEventType.BeaconClick.equals(AdLogEventType
							.get(eventType))
					|| AdLogEventType.ExternalUserSegment.equals(AdLogEventType
							.get(eventType))) {
				AdLogOutput outputs = outputsCollection.getOutput("Actions");
				outputs.setBucketId(Math.abs((int) ((Long.valueOf(userId) % 64))));
				outputs.setTimestamp(timestampStr);
				outputTBLs.add(outputs);
			}

			// 5. collect action data.
			else if (AdLogEventType.ConversionAction.equals(AdLogEventType
					.get(eventType))) {
				AdLogOutput outputs = outputsCollection.getOutput("Actions");
				outputs.setBucketId(Math.abs((int) ((Long.valueOf(userId) % 64))));
				outputs.setTimestamp(timestampStr);
				outputTBLs.add(outputs);
			}

			// 6. collect adview ( both normal & video ads) , adclick, and
			// adselect data.
			else if (AdLogEventType.AdClick.equals(AdLogEventType
					.get(eventType))
					|| AdLogEventType.AdView.equals(AdLogEventType
							.get(eventType))
					|| AdLogEventType.AdSelect.equals(AdLogEventType
							.get(eventType))
					|| AdLogEventType.Video.equals(AdLogEventType
							.get(eventType))) {
				// Filter out empty request id's for RTB Placements
				if (AdLogEventType.AdSelect.equals(AdLogEventType
						.get(eventType)) && requestId.isEmpty()) {
					AdLogOutput outputs = outputsCollection.getOutput("Reminder");
					outputs.setTimestamp(timestampInDayStr);
					outputTBLs.add(outputs);
					return outputTBLs;
				}

				String ad_instance_id = log_record
						.getColumn(AdFields.AD_INSTANCE_ID);
				/*
				 * For impression draft records *append* ad instance id to the
				 * end of the key. The final key looks like :
				 * impression_draft/2010/11/10/15/file.tsv.bz2:aiid:959d32GwCbsH
				 */
				if (!isValidAdInstanceId(ad_instance_id)) {
					AdLogOutput outputs = outputsCollection.getOutput("Reminder");
					outputs.setTimestamp(timestampInDayStr);
					outputTBLs.add(outputs);
					return outputTBLs;
				}
				AdLogOutput outputs = new AdLogOutput();
				outputs.setBucketId(0);
				outputs.setPartition_strategy("daily");
				outputs.setTableName("impression_draft");
				outputs.setTimestamp(timestampInDayStr);
				outputTBLs.add(outputs);
			} else if (AdLogEventType.InteractionTracker.equals(AdLogEventType
					.get(eventType))) {
				AdLogOutput outputs = outputsCollection.getOutput("Interaction_tracker");
				outputs.setTimestamp(timestampInDayStr);
				outputTBLs.add(outputs);
			} else if (AdLogEventType.CookieMatching.equals(AdLogEventType
					.get(eventType))) {
				AdLogOutput outputs = outputsCollection.getOutput("Cookie_matching");
				outputs.setBucketId(Math.abs((int) ((Long.valueOf(userId) % 64))));
				outputs.setTimestamp(timestampStr);
				outputTBLs.add(outputs);
			}
			// ** collect remainder data
			else {
				AdLogOutput outputs = outputsCollection.getOutput("Reminder");
				outputs.setTimestamp(timestampInDayStr);
				outputTBLs.add(outputs);
			}
		}
		// ** collect error records
		catch (Exception e) {
			AdLogOutput outputs = outputsCollection.getOutput("Errors");
			try {
				String timestampStr = log_record
						.getColumn(Fields.SERVER_TIMESTAMP);
				long timestampLong = Long.valueOf(timestampStr);
				String timestampInDayStr = String.valueOf(timestampLong
						- timestampLong % milisecondsInDay);
				outputs.setTimestamp(timestampInDayStr);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			outputTBLs.add(outputs);
		}
		return outputTBLs;
	}

	private RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
			TaskAttemptContext context, String fileName, CamusWrapper value)
			throws IOException, InterruptedException {
		RecordWriterProvider recordWriterProvider = null;
		try {
			// recordWriterProvider =
			// EtlMultiOutputFormat.getRecordWriterProviderClass(context).newInstance();
			Class<RecordWriterProvider> rwp = EtlMultiOutputFormat
					.getRecordWriterProviderClass(context);
			Constructor<RecordWriterProvider> crwp = rwp
					.getConstructor(TaskAttemptContext.class);
			recordWriterProvider = crwp.newInstance(context);
		} catch (InstantiationException e) {
			throw new IllegalStateException(e);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return recordWriterProvider.getDataRecordWriter(context, fileName,
				value, committer);
	}

	private boolean isTestingRecord(String userId, String url, String ip)
			throws Exception {
		String AD_TESTER_USER_ID = "-2";
		// ad tester filter
		// RFI-7405: Filter ad-tester data from ETL.
		if (userId.trim().equals(AD_TESTER_USER_ID)) {
			return true;
		}
		return false;
	}

	private boolean isValidAdInstanceId(String adInstanceId) {
		if (adInstanceId == null || adInstanceId.isEmpty()) {
			return false;
		}
		return true;
	}
}
