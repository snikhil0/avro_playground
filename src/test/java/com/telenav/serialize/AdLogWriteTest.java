package com.telenav.serialize;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdLogWriteTest {

	private DataFileWriter<GenericRecord> dataFileWriter;
	// private final String ADLOG_LOCATION =
	// "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/log.avro";
	// private FileSystem dfs;
	private Schema schema;
	private final long MAX_RECORDS = 100000000L;

	@Before
	public void initialize() {

		try {
			schema = Schema.parse(new File("resources/adlog.avsc"));
			Configuration conf = new Configuration();
			// conf.setBoolean("mapred.compress.map.output", true);
			// conf.set("mapred.map.output.compression.codec",
			// "org.apache.hadoop.io.compress.SnappyCodec");
			// dfs = new DistributedFileSystem();
			// dfs.initialize(URI.create(ADLOG_LOCATION), conf);

			File fs = new File("resources/log_deflate.avro");
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
					schema);
			dataFileWriter = new DataFileWriter<GenericRecord>(writer);
			dataFileWriter.setCodec(CodecFactory.deflateCodec(1));
			dataFileWriter.create(schema, fs);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void test() throws JsonParseException, JsonMappingException,
			IOException {
		File directory = new File("/Users/snikhil/data/logs/");
		File[] files = directory.listFiles(new FileFilter() {

			public boolean accept(File pathname) {
				if (pathname.getName().startsWith("citysearch")) {
					return true;
				}
				return false;
			}
		});

		System.out.println(files.length);
		long count = 0;
		Random fileNum = new Random();
		while (count < MAX_RECORDS) {
			int n = fileNum.nextInt(6);
			BufferedReader rdr = new BufferedReader(new FileReader(files[n]));

			String line = rdr.readLine();

			while (line != null) {
				AdLog log = AdLog.create(line);
				if (log != null) {
					log.initialize(schema, dataFileWriter);
					log.serialize();
					count++;
				}
				line = rdr.readLine();
			}
			rdr.close();
		}

		dataFileWriter.flush();
		System.out.println(count);
	}

	@After
	public void dismantle() throws IOException {
		dataFileWriter.close();
		// dfs.close();
	}

}
