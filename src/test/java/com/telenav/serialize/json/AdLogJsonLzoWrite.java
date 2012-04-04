package com.telenav.serialize.json;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import com.hadoop.compression.lzo.LzoCodec;


/**
 * @author snikhil
 *
 */
public class AdLogJsonLzoWrite {

	public static class Map extends IdentityMapper<LongWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> collector, Reporter reporter)
				throws IOException {
			collector.collect(key, value);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> collector, Reporter reporter)
				throws IOException {
			collector.collect(key, new Text(""));
		}
		
	}
	
	public static void main(String[] args) {
		JobConf conf = new JobConf(AdLogJsonLzoWrite.class);
		conf.setJobName("write gzip files");
		String options[] = new String[2];
		options[0] = "hdfs://hqd-cassandra-01.mypna.com/users/snikhil/input/log.json";
		options[1] = "hdfs://hqd-cassandra-01.mypna.com/users/snikhil/output";
		
		conf.setMapOutputCompressorClass(GzipCodec.class);
		conf.setNumMapTasks(5);
		conf.setNumReduceTasks(0);
		conf.setJarByClass(AdLogJsonLzoWrite.class);
		conf.setMapOutputCompressorClass(GzipCodec.class);
		conf.setMapperClass(Map.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.output.compression.type", "BLOCK");
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		//DistributedCache.createSymlink(conf); 
//		try {
//			DistributedCache.addCacheFile(new URI("hdfs://hqd-cassandra-01.mypna.com/libraries/liblzo2.so.2"), conf);
//		} catch (URISyntaxException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		
		Path outputPath = new Path(options[1]);
		FileInputFormat.setInputPaths(conf, new Path(options[0]));
		FileOutputFormat.setOutputPath(conf,outputPath);
		FileOutputFormat.setCompressOutput(conf, true);
		try {
			outputPath.getFileSystem(conf).delete(outputPath, true);
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
