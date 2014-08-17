package com.prithvi.mapreduce.logprocessing.ipgeo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.prithvi.mapreduce.logprocessing.logwritable.LogInputFormat;

public class GeoLocationDatasetDriver extends Configured implements Tool {

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new GeoLocationDatasetDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.err
					.println("Usage: GeoLocationDatasetDriver <-files cache> <in> <out>");
			System.exit(2);
		}

		Configuration conf = getConf();
		String dbfile = args[2];
		conf.set("maxmind.geo.database.file", dbfile);

		Job job = Job.getInstance(conf);
		job.addCacheFile(new URI(conf.get("fs.defaultFS") + "/"
				+ conf.get("maxmind.geo.database.file") + "#"
				+ conf.get("maxmind.geo.database.file")));

		URI[] cacheFiles = job.getCacheFiles();
		if (cacheFiles != null) {
			for (URI cachefile : cacheFiles) {
				System.out.println("Cache file ------>" + cachefile);
			}
		}

		// job.addCacheFile(new Path(args[2]).toUri()); Use for distributed
		// cache

		// DistributedCache.addCacheFile(new URI(args[2]), conf); // Deprecated
		// distributed cache

		job.setJobName("Add IP location to the Apache log dataset");
		job.setJarByClass(GeoLocationDatasetDriver.class);

		job.setMapperClass(GeoLocationDatasetMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(LogInputFormat.class);
		// job.setNumReduceTasks(0); This job uses default Identity Reducer
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int ret = job.waitForCompletion(true) ? 0 : 1;
		return ret;
	}

}
