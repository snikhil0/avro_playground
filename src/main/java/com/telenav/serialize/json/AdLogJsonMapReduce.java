package com.telenav.serialize.json;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;
import org.json.JSONException;
import org.json.JSONObject;

import com.telenav.serialize.AdLog;

public class AdLogJsonMapReduce extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/mapred-site.xml"));

		Path outputPath = new Path(args[1]);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		conf.setInputFormat(JsonInputFormat.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(JsonRecordMapper.class);
		conf.setReducerClass(JsonRecordReducer.class);
		
		Job job = new Job(conf, "json poiId count");
		job.setJarByClass(AdLogJsonMapReduce.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static class JsonRecordMapper extends MapReduceBase implements Mapper<LongWritable, Text, 
	LongWritable, IntWritable> {
		
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, IntWritable> collector, Reporter reporter)
				throws IOException {
			
			String line = value.toString();
			JSONObject log;
			try {
				log = AdLog.createJson(line);
				collector.collect(new LongWritable(log.getLong("poiId")), new IntWritable(1));
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public static class JsonRecordReducer extends MapReduceBase implements Reducer<LongWritable, IntWritable,
	LongWritable, IntWritable> {

		public void reduce(LongWritable key, Iterator<IntWritable> values,
				OutputCollector<LongWritable, IntWritable> collector, Reporter reporter)
				throws IOException {
			
			int frequency = 0;
			while(values.hasNext()) {
				frequency += values.next().get();
			}
			
			collector.collect(key, new IntWritable(frequency));
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String[] options = new String[2];
		options[0] = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/log_100.json";
		options[1] = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/output/json";
		int status = -1;
		try {
			status = ToolRunner.run(new Configuration(), new AdLogJsonMapReduce(),
					options);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
