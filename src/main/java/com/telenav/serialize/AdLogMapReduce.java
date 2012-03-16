package com.telenav.serialize;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AdLogMapReduce extends Configured implements Tool {

	public final Schema AVRO_SCHEMA;
	public final static Schema OUT_SCHEMA = new Pair<Long, Integer>(0L, Schema.create(Type.LONG), 0, Schema.create(Type.INT)).getSchema();
	private static final Schema KEY_SCHEMA = Schema.create(Type.LONG);
	private static final Schema VAL_SCHEMA = Schema.create(Type.INT);
	
	public AdLogMapReduce() throws IOException {
		AVRO_SCHEMA = Schema.parse(new File("resources/adlog.avsc"));
	}
	
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/mapred-site.xml"));
		
		Job job = new Job(conf, "avro poiId count");
		job.setJarByClass(AdLogMapReduce.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		AvroJob.setInputSchema(conf, AVRO_SCHEMA);
		AvroJob.setMapOutputSchema(conf, OUT_SCHEMA);
        AvroJob.setOutputSchema(conf, Schema.create(Type.STRING));
        
        AvroJob.setMapperClass(conf, AvroRecordMapper.class);        
        AvroJob.setReducerClass(conf, AvroRecordReducer.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
	}

	 public static class AvroRecordMapper extends AvroMapper<AdLog, Pair<Long,Integer>> {
	        public void map(AdLog log, AvroCollector<Pair<Long,Integer>> collector, Reporter reporter) throws IOException {
	            System.out.println(log);
	        	collector.collect( new Pair<Long, Integer>(log.getPoiId(), KEY_SCHEMA, 1, VAL_SCHEMA));
	        }      
	    }

	    public static class AvroRecordReducer extends AvroReducer<Long, Integer, String> {

	        public void reduce(Long poiId, Iterable<Integer> counts,
	                AvroCollector<String> collector,
	                Reporter reporter) throws IOException {
	            int frequency = 0;
	            for (Integer c : counts) {
	                frequency += c;
	            }
	            collector.collect("POIID: " + poiId + "COUNT: " + frequency);
	        }
	    }
	    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] options = new String[2];
		options[0] = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/log.avro";
		options[1] = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/output";
		int status = -1;
		try {
			status = ToolRunner.run(new Configuration(), new AdLogMapReduce(), options);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(status);
	}

}
