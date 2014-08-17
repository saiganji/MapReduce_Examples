package com.prithvi.pig.logprocessing.customreader;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.prithvi.mapreduce.logprocessing.logwritable.LogInputFormat;
import com.prithvi.mapreduce.logprocessing.logwritable.LogRecordReader;
import com.prithvi.mapreduce.logprocessing.logwritable.LogWritable;

/* Script to load data
 * 
register '/tmp/jobs/MapReduce_Examples-0.0.1-SNAPSHOT-jar-with-dependencies.jar';

log_data = LOAD '/user/hdfs/input/mock_apache_pool-1-thread-1.data' USING com.prithvi.pig.logprocessing.customreader.WebLogLoader As (ipAddr:chararray, clientIdentity:chararray, userId:chararray, timeStamp:chararray, requestType:chararray, requestPage:chararray,  httpProt:chararray, responseCode:int, reposnseSize:int, referrer:chararray, userAgent:chararray);
A = LIMIT log_data 10;
dump A;

 */

public class WebLogLoader extends LoadFunc {
	
	private LogRecordReader reader;
	
	private LogWritable lw = null;
	private TupleFactory tf = TupleFactory.getInstance();;
	
	@Override
	public InputFormat getInputFormat() throws IOException {
		
		return new LogInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		Tuple t = null;
		try {
			if (!reader.nextKeyValue()) return null;
			lw = (LogWritable)reader.getCurrentValue();
			
			t = tf.newTuple(11);
			
			t.set(0, lw.getOriginatingIP().toString());
			t.set(1, lw.getClientIdentity().toString());
			t.set(2, lw.getUserId().toString());
			t.set(3, lw.getTimeStamp().toString());
			t.set(4, lw.getRequestType().toString());
			t.set(5, lw.getRequestPage().toString());
			t.set(6, lw.getHttpProtocolVersion().toString());
			t.set(7, lw.getResponseCode());
			t.set(8, lw.getResponseSize());
			t.set(9, lw.getReferrer().toString());
			t.set(10, lw.getUserAgent().toString());
			
		} 
		catch (InterruptedException e) {
			
			e.printStackTrace();
		}
		
		return t;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit pigsplit)
			throws IOException {
		this.reader = (LogRecordReader) reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {

		FileInputFormat.setInputPaths(job, location);
		
	}

}
