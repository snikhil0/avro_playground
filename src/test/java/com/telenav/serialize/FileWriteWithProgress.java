package com.telenav.serialize;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileWriteWithProgress {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			InputStream in = new BufferedInputStream(new FileInputStream("/Users/snikhil/data/input.txt"));
			String dst =  "hdfs://hqd-cassandra-01.mypna.com/user/snikhil/input/input_copy.txt";
			Configuration conf = new Configuration();
			DistributedFileSystem dfs = new DistributedFileSystem();
			dfs.initialize(URI.create(dst), conf);
			OutputStream os = null;
			if(!dfs.exists(new Path(dst))) {
				os = dfs.create(new Path(dst), new Progressable() {
					
					public void progress() {
					System.out.print(".");
						
					}
				});
				
			} else {
				os = dfs.append(new Path(dst), 4096, new Progressable() {
					
					public void progress() {
					System.out.print(".");
						
					}
				});
			}
			
			
			IOUtils.copyBytes(in, os, 4096, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
