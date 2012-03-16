package com.telenav.serialize;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdLogWriteTest {

	private DataFileWriter<GenericRecord> dataFileWriter;
	private final String ADLOG_LOCATION = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/log.avro";
	private DistributedFileSystem dfs;
	private Schema schema;
	
	@Before
	public void initialize() {
		
		try {
			schema = Schema.parse(new File("resources/adlog.avsc"));
			Configuration conf = new Configuration();
			dfs = new DistributedFileSystem();
			dfs.initialize(URI.create(ADLOG_LOCATION), conf);
			
			//File fs = new File("resources/log.avro");
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
			dataFileWriter = new DataFileWriter<GenericRecord>(writer);
			dataFileWriter.create(schema, dfs.create(new Path(ADLOG_LOCATION)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void test() throws JsonParseException, JsonMappingException, IOException {
		BufferedReader rdr = new BufferedReader(new 
				FileReader("/Users/snikhil/data/logs/citysearch-response.log.2011-12-01"));
		
		String line = rdr.readLine();
		while(line != null) {
			AdLog log = AdLog.create(line);
			log.initialize(schema, dataFileWriter);
			log.serialize();
			line = rdr.readLine();
		}
	}
	
	@After
	public void dismantle() throws IOException {
		dataFileWriter.close();
		dfs.close();
	}

}
