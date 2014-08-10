package com.prithvi.mapreduce.logprocessing.customreader;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.prithvi.mapreduce.logprocessing.logwritable.LogWritable;

public class CustomReaderLogReducer extends
		Reducer<LogWritable, IntWritable, Text, IntWritable> {

	private IntWritable count = new IntWritable();
	private Text result = new Text();

	public void reduce(LogWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(key.getRequestPage() + "    " + key.getTimeStamp());
		count.set(sum);
		context.write(result, count);
	}

}
