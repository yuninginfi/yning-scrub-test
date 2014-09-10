package com.linkedin.camus.etl.kafka;

import com.linkedin.camus.etl.kafka.common.DateUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

public class MultifileWritterTest extends Configured implements Tool {

	public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
	public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";
	public static final String ETL_COUNTS_PATH = "etl.counts.path";
	public static final String ETL_KEEP_COUNT_FILES = "etl.keep.count.files";
	public static final String ETL_BASEDIR_QUOTA_OVERIDE = "etl.basedir.quota.overide";
	public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";
	public static final String ZK_AUDIT_HOSTS = "zookeeper.audit.hosts";
	public static final String KAFKA_MONITOR_TIER = "kafka.monitor.tier";
	public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
	public static final String BROKER_URI_FILE = "brokers.uri";
	public static final String POST_TRACKING_COUNTS_TO_KAFKA = "post.tracking.counts.to.kafka";
	public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "kafka.fetch.request.max.wait";
	public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "kafka.fetch.request.min.bytes";
	public static final String KAFKA_FETCH_REQUEST_CORRELATION_ID = "kafka.fetch.request.correlationid";
	public static final String KAFKA_CLIENT_NAME = "kafka.client.name";
	public static final String KAFKA_FETCH_BUFFER_SIZE = "kafka.fetch.buffer.size";
	public static final String KAFKA_FETCH_MAX_ROWS = "kafka.fetch.max.rows";
	public static final String KAFKA_BROKERS = "kafka.brokers";
	public static final String KAFKA_HOST_URL = "kafka.host.url";
	public static final String KAFKA_HOST_PORT = "kafka.host.port";
	public static final String KAFKA_TIMEOUT_VALUE = "kafka.timeout.value";
	public static final String LOG4J_CONFIGURATION = "log4j.configuration";
	public static final String MAX_OPEN_FILE_WRITTER = "max.open.file.writter";
	private static org.apache.log4j.Logger log;

	private final Properties props;

	private DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter(
			"YYYY-MM-dd-HH-mm-ss", DateTimeZone.UTC);

	public MultifileWritterTest() throws IOException {
		this(new Properties());
	}

	public MultifileWritterTest(Properties props) throws IOException {
		this(props, org.apache.log4j.Logger
				.getLogger(MultifileWritterTest.class));
	}

	public MultifileWritterTest(Properties props, Logger log)
			throws IOException {
		this.props = props;
		this.log = log;
	}

	private Job createJob(Properties props) throws IOException {
		Job job;
		if (getConf() == null) {
			setConf(new Configuration());
		}

		populateConf(props, getConf(), log);

		job = new Job(getConf());
		job.setJarByClass(MultifileWritterTest.class);

		if (job.getConfiguration().get("camus.job.name") != null) {
			job.setJobName(job.getConfiguration().get("camus.job.name"));
		} else {
			job.setJobName("Camus Job");
		}

		return job;
	}

	public static void populateConf(Properties props, Configuration conf,
			Logger log) throws IOException {
		for (Object key : props.keySet()) {
			conf.set(key.toString(), props.getProperty(key.toString()));
		}

		FileSystem fs = FileSystem.get(conf);

		String hadoopCacheJarDir = conf.get("hdfs.default.classpath.dir", null);
		if (hadoopCacheJarDir != null) {
			FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

			if (status != null) {
				for (int i = 0; i < status.length; ++i) {
					if (!status[i].isDir()) {
						log.info("Adding Jar to Distributed Cache Archive File:"
								+ status[i].getPath());

						DistributedCache.addFileToClassPath(
								status[i].getPath(), conf, fs);
					}
				}
			} else {
				System.out.println("hdfs.default.classpath.dir "
						+ hadoopCacheJarDir + " is empty.");
			}
		}

		// Adds External jars to hadoop classpath
		String externalJarList = conf.get("hadoop.external.jarFiles", null);
		if (externalJarList != null) {
			String[] jarFiles = externalJarList.split(",");
			for (String jarFile : jarFiles) {
				log.info("Adding external jar File:" + jarFile);
				DistributedCache
						.addFileToClassPath(new Path(jarFile), conf, fs);
			}
		}
	}

	public void run() throws Exception {
		Job job = createJob(props);

		Configuration conf = job.getConfiguration();

		if (getLog4jConfigure(job)) {
			DOMConfigurator.configure("log4j.xml");
		}
		FileSystem.get(job.getConfiguration());

		String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
		String DEFAULT_RECORD_DELIMITER = "";
		job.getConfiguration().get(ETL_OUTPUT_RECORD_DELIMITER,
				DEFAULT_RECORD_DELIMITER);

		List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>();

		// Create 1000 writers
		for (int i = 0; i < 1000; i++) {
			// Get the filename for this RecordWriter.
			Path path = new Path("/user/yning/test/", "file" + String.valueOf(i));

			@SuppressWarnings("deprecation")
			final SequenceFile.Writer writer = SequenceFile.createWriter(
					path.getFileSystem(conf), conf, path, LongWritable.class,
					Text.class);

		}
		
/*
		writer.append(new LongWritable(1l), new Text("123456"));
		writer.close();*/

	}

	public static void main(String[] args) throws Exception {
		MultifileWritterTest job = new MultifileWritterTest();
		ToolRunner.run(job, args);
	}

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption("p", true, "properties filename from the classpath");
		options.addOption("P", true, "external properties filename");

		options.addOption(OptionBuilder.withArgName("property=value")
				.hasArgs(2).withValueSeparator()
				.withDescription("use value for given property").create("D"));

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		if (!(cmd.hasOption('p') || cmd.hasOption('P'))) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("CamusJob.java", options);
			return 1;
		}

		if (cmd.hasOption('p'))
			props.load(this.getClass().getClassLoader()
					.getResourceAsStream(cmd.getOptionValue('p')));

		if (cmd.hasOption('P')) {
			File file = new File(cmd.getOptionValue('P'));
			FileInputStream fStream = new FileInputStream(file);
			props.load(fStream);
		}

		props.putAll(cmd.getOptionProperties("D"));

		run();
		return 0;
	}

	public static boolean getLog4jConfigure(JobContext job) {
		return job.getConfiguration().getBoolean(LOG4J_CONFIGURATION, false);
	}
}
