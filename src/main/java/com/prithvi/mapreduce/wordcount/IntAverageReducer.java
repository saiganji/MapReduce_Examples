package com.prithvi.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntAverageReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable average = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0, count = 0;
		for (IntWritable val : values) {
			sum = sum + val.get();
			count = count + 1;
		}
		average.set(sum / count);
		context.write(key, average);
	}
}
