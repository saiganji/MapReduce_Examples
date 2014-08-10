package com.prithvi.mapreduce.logprocessing.statuscount;

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

public class LogStatusCountDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new LogStatusCountDriver(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: StatusCount <in> <out>");
			System.exit(2);
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJobName("log status count");
		job.setJarByClass(LogStatusCountDriver.class);
		job.setMapperClass(LogStatusCountMapper.class);
		job.setReducerClass(LogStatusCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int ret = job.waitForCompletion(true) ? 0 : 1;
		return ret;
	}

}
