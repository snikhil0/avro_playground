package com.telenav.serialize;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AdLogMapReduce extends Configured implements Tool {

	public final Schema AVRO_SCHEMA;
	public final static Schema OUT_SCHEMA = new Pair<Long, Integer>(0L,
			Schema.create(Type.LONG), 0, Schema.create(Type.INT)).getSchema();
	private static final Schema KEY_SCHEMA = Schema.create(Type.LONG);
	private static final Schema VAL_SCHEMA = Schema.create(Type.INT);
	//private final static String ADLOG_OUTPUT_LOCATION = "hdfs://hqd-cassandra-01.mypna.com/users/snikhil/output/part-00000.avro";
	
	public AdLogMapReduce() throws IOException {
		AVRO_SCHEMA = Schema.parse(new File("resources/adlog.avsc"));
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.setJarByClass(AdLogMapReduce.class);
		//conf.setMapperClass(AvroRecordMapper.class);
		//conf.setReducerClass(AvroRecordReducer.class);
		conf.setJobName("avro_poi_count");
		conf.setInputFormat(AvroInputFormat.class);
		conf.setOutputFormat(AvroOutputFormat.class);
		
//		conf.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
//		conf.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
//		conf.addResource(new Path("$HADOOP_HOME/conf/mapred-site.xml"));
		//conf.setBoolean("mapred.compress.map.output", true);
		//conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		//conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DeflateCodec");
		
		Job job = new Job(conf, "avro poiId count");
		job.setJarByClass(AdLogMapReduce.class);
		Path outputPath = new Path(args[1]);
		job.setJobName("poiId_count");

		AvroInputFormat.setInputPaths(conf, args[0]);
		AvroOutputFormat.setOutputPath(conf, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);

		AvroJob.setInputSchema(conf, AVRO_SCHEMA);
		AvroJob.setOutputSchema(conf, OUT_SCHEMA);
		//AvroJob.setOutputCodec(conf, "deflate");

		AvroJob.setMapperClass(conf, AvroRecordMapper.class);
		AvroJob.setReducerClass(conf, AvroRecordReducer.class);

		JobClient.runJob(conf);
		return 1;
	}

	public static class AvroRecordMapper extends
			AvroMapper<GenericData.Record, Pair<Long, Integer>> {
		public void map(GenericData.Record log,
				AvroCollector<Pair<Long, Integer>> collector, Reporter reporter)
				throws IOException {
			collector.collect(new Pair<Long, Integer>((Long) log.get("poiId"),
					KEY_SCHEMA, 1, VAL_SCHEMA));
		}
	}

	public static class AvroRecordReducer extends
			AvroReducer<Long, Integer, Pair<Long, Integer>> {

		public void reduce(Long poiId, Iterable<Integer> counts,
				AvroCollector<Pair<Long, Integer>> collector, Reporter reporter)
				throws IOException {
			int frequency = 0;
			for (Integer c : counts) {
				frequency += c;
			}
			collector.collect(new Pair<Long, Integer>(poiId, KEY_SCHEMA, frequency, VAL_SCHEMA));
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String[] options = new String[2];
		options[0] = "hdfs://hqd-cassandra-01.mypna.com/users/snikhil/input/log.avro";
		options[1] = "hdfs://hqd-cassandra-01.mypna.com/users/snikhil/output";
		int status = -1;
		long startTime = System.currentTimeMillis();
		try {
			status = ToolRunner.run(new Configuration(), new AdLogMapReduce(),
					options);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println((System.currentTimeMillis() - startTime)/1000);
		// Read the number of visits per poiId
//		Configuration conf = new Configuration();
//		conf.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
//		conf.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
//		
//		FileSystem dfs = new DistributedFileSystem();
//		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(new Pair<Long, Integer>(0L, KEY_SCHEMA, 0, VAL_SCHEMA).getSchema());
//		DataFileReader<GenericRecord> dataFileReader = null;
//		try {
//			dfs.initialize(URI.create(ADLOG_OUTPUT_LOCATION), conf);
//			FsInput f = new FsInput(new Path(ADLOG_OUTPUT_LOCATION), conf); 
//			dataFileReader = new DataFileReader<GenericRecord>(f, reader);
//			GenericRecord record = null;
//			while(dataFileReader.hasNext()) {
//				record = dataFileReader.next(record);
//				System.out.println(record);
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			dataFileReader.close();
//		}
		
		System.exit(status);
		
		
	}

}
