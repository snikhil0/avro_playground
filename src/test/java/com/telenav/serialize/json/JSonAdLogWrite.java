/**
 * 
 */
package com.telenav.serialize.json;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.telenav.serialize.AdLog;
import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat;
import com.twitter.elephantbird.pig.load.LzoJsonLoader;

/**
 * @author snikhil
 *
 */
public class JSonAdLogWrite {

	private final String ADLOG_LOCATION = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/log.json";
	private FileSystem dfs;
	private BufferedWriter writer;
	private final long MAX_RECORDS = 100000000L;
	@Before
	public void initialize() {
		try {
			//dfs = new DistributedFileSystem();
			//Configuration conf = new Configuration();
			//dfs.initialize(URI.create(ADLOG_LOCATION), conf );
			File fs = new File("/Users/snikhil/data/log_store/log.json");
			writer = new BufferedWriter(new FileWriter(fs));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void test() throws IOException, JSONException {
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
				JSONObject log = AdLog.createJson(line);
				
				if (log != null) {
					JSONObjectWritable obj = new JSONObjectWritable(log);
					obj.write(writer);
					writer.newLine();
					count++;
				}
				line = rdr.readLine();
			}
			rdr.close();
		}

		writer.flush();
		System.out.println(count);
		writer.close();

	}
}
