package com.prithvi.mapreduce.logprocessing.customreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.prithvi.mapreduce.logprocessing.logwritable.LogInputFormat;
import com.prithvi.mapreduce.logprocessing.logwritable.LogWritable;
import com.prithvi.mapreduce.logprocessing.partitioner.PartitionerLogIP;

public class CustomReaderLogDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new CustomReaderLogDriver(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: OutputCustomLogDriver <in> <out>");
			System.exit(2);
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJobName("Log request page count");
		job.setJarByClass(CustomReaderLogDriver.class);

		job.setMapperClass(CustomReaderLogMapper.class);
		job.setReducerClass(CustomReaderLogReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(LogWritable.class);
		job.setPartitionerClass(PartitionerLogIP.class);
		job.setInputFormatClass(LogInputFormat.class);
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int ret = job.waitForCompletion(true) ? 0 : 1;
		return ret;
	}

}
