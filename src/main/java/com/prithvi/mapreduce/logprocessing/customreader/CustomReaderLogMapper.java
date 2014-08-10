package com.prithvi.mapreduce.logprocessing.customreader;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.prithvi.mapreduce.logprocessing.logwritable.LogWritable;

public class CustomReaderLogMapper extends
		Mapper<Text, LogWritable, LogWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private LogWritable log = new LogWritable();
	Text timeStamp = new Text();
	Text requestPage = new Text();

	public void map(Text key, LogWritable value, Context context)
			throws IOException, InterruptedException {
		timeStamp = value.getTimeStamp();
		requestPage = value.getRequestPage();

		if (timeStamp != null) {
			log.setTimeStamp(timeStamp);
		} else {
			timeStamp.set("Unknown Time");
			log.setTimeStamp(timeStamp);
		}
		if (requestPage != null) {
			log.setRequestPage(requestPage);
		} else {
			requestPage.set("Unknown Page");
			log.setRequestPage(requestPage);
		}
		context.write(log, one);
	}
}
