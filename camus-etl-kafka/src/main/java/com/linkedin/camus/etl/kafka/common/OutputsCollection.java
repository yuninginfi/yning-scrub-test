package com.linkedin.camus.etl.kafka.common;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.camus.etl.AdLogOutput;

public class OutputsCollection {
	private static Map<String, AdLogOutput> outputs;

	public void ini() {
		outputs = new HashMap<String, AdLogOutput> ();
		
		// Bots table
		AdLogOutput output = new AdLogOutput();
		output.setBucketId(0);
		output.setPartition_strategy("daily");
		output.setTableName("Bots");
		outputs.put("Bots", output);

		// Clicks
		output = new AdLogOutput();
		output.setBucketId(0);
		output.setPartition_strategy("daily");
		output.setTableName("Clicks");
		outputs.put("Clicks", output);

		// Rtb_video
		output = new AdLogOutput();
		output.setBucketId(0);
		output.setPartition_strategy("daily");
		output.setTableName("Rtb_video");
		outputs.put("Rtb_video", output);

		// Actions
		output = new AdLogOutput();
		output.setPartition_strategy("hourly");
		output.setTableName("Actions");
		outputs.put("Actions", output);

		// Reminder
		output = new AdLogOutput();
		output.setBucketId(0);
		output.setPartition_strategy("daily");
		output.setTableName("Reminder");
		outputs.put("Reminder", output);
		
		// Impression_draft table
		output = new AdLogOutput();
		output.setBucketId(0);
		output.setPartition_strategy("daily");
		output.setTableName("Impression_draft");
		outputs.put("Impression_draft", output);

		// Interaction_tracker
		output = new AdLogOutput();
		output.setBucketId(0);
		output.setPartition_strategy("daily");
		output.setTableName("Interaction_tracker");
		outputs.put("Interaction_tracker", output);

		// Cookie_matching
		output = new AdLogOutput();
		output.setPartition_strategy("hourly");
		output.setTableName("Cookie_matching");
		outputs.put("Cookie_matching", output);

		// Cookies
		output = new AdLogOutput();
		output.setPartition_strategy("hourly");
		output.setTableName("Cookies");
		outputs.put("Cookies", output);

		// Errors
		output = new AdLogOutput();
		output.setBucketId(0);
		output.setPartition_strategy("daily");
		output.setTableName("Errors");
		outputs.put("Errors", output);
	}

	public AdLogOutput getOutput(String tableName) {
		return outputs.get(tableName);
	}
}
