package com.prithvi.mapreduce.logprocessing.statuscount.counters.maponly;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MonlyLogStatusCountMapper extends
		Mapper<Object, Text, NullWritable, NullWritable> {

	public static enum Counter {
		OK, UNAVAILABLE, NOTFOUND, UNKNOWN
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String string = value.toString();
		String[] parts = string.split(" ");
		String status = parts[7];

		if (status.equalsIgnoreCase("404")) {
			context.getCounter(Counter.NOTFOUND).increment(1);
		} else if (status.equalsIgnoreCase("200")) {
			context.getCounter(Counter.OK).increment(1);
		} else if (status.equalsIgnoreCase("503")) {
			context.getCounter(Counter.UNAVAILABLE).increment(1);
		} else {
			context.getCounter(Counter.UNKNOWN).increment(1);
		}
	}

}
