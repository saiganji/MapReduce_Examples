package com.prithvi.mapreduce.logprocessing.statuscount.counters.maponly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MonlyLogStatusCountDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MonlyLogStatusCountDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		/*
		 * if (args.length != 1) {
		 * System.err.println("Usage: MonlyStatusCountDriver <in>");
		 * System.exit(2); }
		 */

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJobName("map only log status count");
		job.setJarByClass(MonlyLogStatusCountDriver.class);
		job.setMapperClass(MonlyLogStatusCountMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int ret = job.waitForCompletion(true) ? 0 : 1;

		long okcounter = job.getCounters()
				.findCounter(MonlyLogStatusCountMapper.Counter.OK).getValue();
		long notfoundcounter = job.getCounters()
				.findCounter(MonlyLogStatusCountMapper.Counter.NOTFOUND)
				.getValue();
		long unavailablecounter = job.getCounters()
				.findCounter(MonlyLogStatusCountMapper.Counter.UNAVAILABLE)
				.getValue();
		long unknowncounter = job.getCounters()
				.findCounter(MonlyLogStatusCountMapper.Counter.UNKNOWN)
				.getValue();
		System.out.println("200 Ok count: " + okcounter);
		System.out.println("404 NotFound count: " + notfoundcounter);
		System.out.println("503 Unavailable count: " + unavailablecounter);
		System.out.println("Unknown count: " + unknowncounter);
		return ret;
	}

}
