package com.telenav.serialize.json;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonRecordWriter extends RecordWriter<LongWritable, IntWritable> {

	private FSDataOutputStream fsout;
	
	public JsonRecordWriter(FileSplit split, JobConf conf) throws IOException {
		Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);
        if(fs.exists(file)) {
        	fsout = fs.append(file);
        } else {
        	fsout = fs.create(file);
        }
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		synchronized (this) {
			fsout.close();
		}

	}

	@Override
	public void write(LongWritable key, IntWritable value) throws IOException,
			InterruptedException {
		JSONObject object = new JSONObject();
		try {
			object.put("poiId", key);
			object.put("count", value);
			fsout.writeUTF(object.toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
