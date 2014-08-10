package com.prithvi.mapreduce.logprocessing.statuscount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogStatusCountMapper extends
		Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String string = value.toString();
		String[] parts = string.split(" ");
		String status = parts[7];

		if (status.equalsIgnoreCase("404")) {
			word.set("404 Not Found");
			context.write(word, one);
		} else if (status.equalsIgnoreCase("200")) {
			word.set("200 OK");
			context.write(word, one);
		} else if (status.equalsIgnoreCase("503")) {
			word.set("503 Service Unavailable");
			context.write(word, one);
		} else {
			word.set("Unknown");
			context.write(word, one);
		}
	}

}
