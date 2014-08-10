package com.prithvi.mapreduce.logprocessing.ipgeo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.prithvi.mapreduce.logprocessing.logwritable.LogWritable;

public class GeoLocationDatasetMapper extends
		Mapper<Text, LogWritable, NullWritable, Text> {

	Text data = new Text();
	String ipLocation;
	LookupService cl;

	public void setup(Context context) throws IOException {
		// URI[] uriList = DistributedCache.getCacheFiles(
		// context.getConfiguration() );

		// String database_path = uriList[0].getPath();
		// File d = new File(database_path.toString()).toURI();

		/*
		 * URI[] uriList = context.getCacheFiles(); Path database_path = new
		 * Path(uriList[0].toString());
		 * System.out.println("_________"+database_path.toString());
		 */// Use for distributed cache

		// Path[] localPaths = context.getLocalCacheFiles();
		// Path[] uriList =
		// DistributedCache.getLocalCacheFiles(context.getConfiguration());
		// String database_path = uriList[0].getName();

		Configuration conf = context.getConfiguration();
		String database_path = conf.get("maxmind.geo.database.file");

		cl = new LookupService(database_path, LookupService.GEOIP_MEMORY_CACHE
				| LookupService.GEOIP_CHECK_CACHE);

	}

	public void map(Text key, LogWritable value, Context context)
			throws IOException, InterruptedException {
		Location location = cl.getLocation(value.getOriginatingIP().toString());
		ipLocation = location.countryName;
		data.set(value.toString() + " " + ipLocation);

		context.write(NullWritable.get(), data);
	}
}