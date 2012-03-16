package com.telenav.serialize;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdLogReadTest {

	private DataFileReader<GenericRecord> dataFileReader;
	private final String ADLOG_LOCATION = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/log.avro";
	private DistributedFileSystem dfs;
	private Schema schema;
	
	@SuppressWarnings("deprecation")
	@Before
	public void initialize() {
		
		try {
			schema = Schema.parse(new File("resources/adlog.avsc"));
			Configuration conf = new Configuration();
			
			dfs = new DistributedFileSystem();
			dfs.initialize(URI.create(ADLOG_LOCATION), conf);
			
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			
			FsInput f = new FsInput(new Path(ADLOG_LOCATION), conf); 
			dataFileReader = new DataFileReader<GenericRecord>(f, reader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void test() throws IOException {
		GenericRecord record = null;
		while(dataFileReader.hasNext()) {
			record = dataFileReader.next(record);
			System.out.println(record);
		}
	}

	@After
	public void after() throws IOException {
		dataFileReader.close();
		dfs.close();
	}
}
