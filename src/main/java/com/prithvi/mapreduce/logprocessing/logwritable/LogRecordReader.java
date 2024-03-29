package com.prithvi.mapreduce.logprocessing.logwritable;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class LogRecordReader extends RecordReader<Text, LogWritable> {
	LineRecordReader lineReader;
	private Text key;
	private LogWritable value;
	String data;
	String originatingIP;
	String clientIdentity;
	String userId;
	String timeStamp;
	String requestType;
	String requestPage;
	String httpProtocolVersion;
	int responseCode;
	int responseSize;
	String referrer;
	String userAgent;

	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public LogWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public void initialize(InputSplit genericsplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		lineReader = new LineRecordReader();
		lineReader.initialize(genericsplit, context);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (!lineReader.nextKeyValue()) {
			return false;
		}

		data = lineReader.getCurrentValue().toString();
		if (data != null) {
			String[] parts = data.split(" ");
			key = new Text();
			key.set(parts[0]);
			Pattern httpLogPattern = Pattern
					.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\S]+)\\] \\\"([^\\\"][\\w]+) ([\\S]+) ([\\S]+)\\\" ([\\d]{3}) ([\\d]+) \\\"([^\\\"]+)\\\" \\\"([^\\\"]+)\\\"");
			Matcher matcher = httpLogPattern.matcher(lineReader
					.getCurrentValue().toString());
			if (!matcher.matches()) {
				System.out
						.println("Bad Record:" + lineReader.getCurrentValue());
				return nextKeyValue();
			}
			originatingIP = matcher.group(1);
			clientIdentity = matcher.group(2);
			userId = matcher.group(3);
			timeStamp = matcher.group(4);
			requestType = matcher.group(5);
			requestPage = matcher.group(6);
			httpProtocolVersion = matcher.group(7);
			try {
				responseCode = Integer.parseInt(matcher.group(8));
				responseSize = Integer.parseInt(matcher.group(9));
			} catch (Exception e) {
				responseCode = 000;
				responseSize = 0;
			}
			referrer = matcher.group(10);
			userAgent = matcher.group(11);
			value = new LogWritable();
			/*
			 * value.set(parts[0], parts[1], parts[2], parts[3], parts[4],
			 * parts[5], parts[6], Integer.parseInt(parts[7]),
			 * Integer.parseInt(parts[8]), parts[9], parts[10]);
			 */
			value.set(originatingIP, clientIdentity, userId, timeStamp,
					requestType, requestPage, httpProtocolVersion,
					responseCode, responseSize, referrer, userAgent);
		} else {
			key = new Text();
			value = new LogWritable();
		}
		return true;
	}
}
