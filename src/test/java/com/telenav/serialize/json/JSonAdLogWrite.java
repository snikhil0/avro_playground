/**
 * 
 */
package com.telenav.serialize.json;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.codehaus.jackson.JsonNode;
import org.junit.Before;
import org.junit.Test;

import com.telenav.serialize.AdLog;

/**
 * @author snikhil
 *
 */
public class JSonAdLogWrite {

	private final String ADLOG_LOCATION = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/log.json";
	private DistributedFileSystem dfs;
	private BufferedWriter writer;
	@Before
	public void initialize() {
		try {
			dfs = new DistributedFileSystem();
			Configuration conf = new Configuration();
			dfs.initialize(URI.create(ADLOG_LOCATION), conf );
			writer = new BufferedWriter(new OutputStreamWriter(dfs.create(new Path(ADLOG_LOCATION), true)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void test() throws IOException {
		BufferedReader rdr = new BufferedReader(new 
				FileReader("/Users/snikhil/data/logs/citysearch-response.log.2011-12-01"));
		
		String line = rdr.readLine();
		while(line != null) {
			JsonNode log = AdLog.createJson(line);
			writer.append(log.toString());
			line = rdr.readLine();
		}
		
		writer.close();
	}

}
