package com.prithvi.mapreduce.logprocessing.logwritable;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LogInputFormat extends FileInputFormat<Text, LogWritable> {

	@Override
	public RecordReader<Text, LogWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		/*
		 * RecordReader<Text,LogWritable> recordReader =
		 * (RecordReader<Text,LogWritable>)new LogRecordReader();
		 * recordReader.initialize(split, context); return recordReader;
		 */

		return new LogRecordReader();
	}

}
