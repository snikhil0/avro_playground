package com.telenav.serialize.json;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class JsonOutputFormat extends FileOutputFormat<LongWritable, Text> {

	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(FileSystem fs, JobConf conf,
			String line, Progressable progress) throws IOException {
		return null;
	}

}
