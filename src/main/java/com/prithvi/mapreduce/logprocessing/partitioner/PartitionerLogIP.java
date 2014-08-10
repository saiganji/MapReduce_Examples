package com.prithvi.mapreduce.logprocessing.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.prithvi.mapreduce.logprocessing.logwritable.LogWritable;

public class PartitionerLogIP extends Partitioner<LogWritable, IntWritable> {

	@Override
	public int getPartition(LogWritable key, IntWritable value,
			int numReduceTasks) {

		return key.getResponseCode().hashCode() % numReduceTasks;

	}
}
