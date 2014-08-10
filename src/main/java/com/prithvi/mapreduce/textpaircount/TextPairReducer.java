package com.prithvi.mapreduce.textpaircount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TextPairReducer extends
		Reducer<TextPairWritable, IntWritable, Text, IntWritable> {
	private Text pair = new Text();
	private IntWritable count = new IntWritable();

	public void reduce(TextPairWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum = sum + value.get();
		}
		pair.set(key.getFirstString() + " " + key.getSecondString());
		context.write(pair, count);
	}
}
