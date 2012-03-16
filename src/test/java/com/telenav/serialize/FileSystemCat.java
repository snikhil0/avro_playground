package com.telenav.serialize;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

/**
 * Hello world!
 * 
 */
public class FileSystemCat {

	public static void main(String[] args) throws Exception {
		String url = "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/input.txt";
		Path path = new Path(url);
		Configuration conf = new Configuration();
		conf.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
	
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem dfs = new DistributedFileSystem();
		dfs.initialize(URI.create(url), conf);
		
		URI uri = path.toUri();
		System.out.println(uri.getScheme());
		System.out.println(fs.getUri().getScheme());
		System.out.println(dfs.getUri().getScheme());
		InputStream in = null;
		try {
			in = dfs.open(path);
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}