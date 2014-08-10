package com.prithvi.mapreduce.logprocessing.logwritable;

import java.io.IOException;

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
			value = new LogWritable();

			value.set(parts[0], parts[1], parts[2], parts[3], parts[4],
					parts[5], parts[6], Integer.parseInt(parts[7]),
					Integer.parseInt(parts[8]), parts[9], parts[10]);

		} else {

			key = new Text();
			value = new LogWritable();
		}

		return true;
	}

}
