package com.telenav.serialize.json;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class AdLogJsonMapReduce extends Configured implements Tool {

	private final static String ADLOG_OUTPUT_LOCATION = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/output/part-00000.json";
	
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/mapred-site.xml"));

		Job job = new Job(conf, "json poiId count");
		job.setJarByClass(AdLogJsonMapReduce.class);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		conf.setInputFormat(JsonInputFormat.class);
		conf.setOutputFormat(JsonOutputFormat.class);
		
		conf.setMapperClass(JsonRecordMapper.class);
		
		return 0;
	}

}
