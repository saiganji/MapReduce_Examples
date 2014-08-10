package com.prithvi.mapreduce.textpaircount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TextPairMapper extends
		Mapper<IntWritable, Text, TextPairWritable, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private TextPairWritable textpair = new TextPairWritable();;
	String first, second;

	public void map(IntWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		StringTokenizer stkr = new StringTokenizer(values.toString());
		while (stkr.hasMoreElements()) {
			first = stkr.nextToken();
			second = stkr.nextToken();
		}
		textpair.setFirstString(first);
		textpair.setSecondString(second);
		context.write(textpair, one);
	}
}